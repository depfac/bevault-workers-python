"""Load, resolve, and retrieve configured data store instances."""

import os
import threading
import importlib
import logging
import copy
from urllib.parse import urlparse
from importlib.metadata import entry_points

from bevault_workers.utils.config_loader import load_store_config
from .base_store import FileStore

logger = logging.getLogger(__name__)

# The canonical package where the library's built-in stores live
LIB_STORES_PKG = "bevault_workers.stores"


class UnknownStoreError(KeyError):
    """Raised when :meth:`StoreRegistry.get` is called with an unregistered store name.

    Subclasses :class:`KeyError` so ``except KeyError`` continues to catch missing
    stores, while :func:`str` on the exception yields a full explanation for
    orchestrator ``cause`` / logging.
    """

    def __init__(self, name: str):
        self.name = name
        self._detail = (
            f"Unknown store {name!r}: not found in the store registry. "
            "Verify the name exists in config.json, the STORE_CONFIG environment variable, "
            "or States store sync definitions."
        )
        super().__init__(name)

    def __str__(self) -> str:
        return self._detail


class StoreRegistry:
    """Registry and loader for data stores.

    Supported ways to reference a store 'type' in config:
      1) Bare name (e.g., 'postgresql', 'sqlserver', 's3'):
           - Tries bevault_workers.stores.<type> first (library)
           - Then tries stores.<type> (user project)
           - Then tries any base packages listed in env bevault_workers_STORE_PATHS
      2) Fully-qualified module path (e.g., 'my_project.custom.stores.pg'):
           - Imports that module and looks for a class named 'Store'
      3) Explicit module and class using colon (e.g., 'my_pkg.mod:MyStoreClass'):
           - Imports module and returns the specified class
      4) Python entry points (group: 'bevault_workers.stores'):
           - If an entry point name matches the type, loads it (class or module)
    """

    _lock = threading.RLock()
    _instances = {}
    _metadata = {}
    _definitions = []
    _shared_definitions = None
    _shared_metadata = None
    _shared_lock = None
    _loaded = False

    @classmethod
    def clear(cls):
        """Clear cache (useful in tests)."""
        with cls._lock:
            cls._instances.clear()
            cls._metadata.clear()
            cls._definitions.clear()
            cls._shared_definitions = None
            cls._shared_metadata = None
            cls._shared_lock = None
            cls._loaded = False

    @classmethod
    def configure_shared_state(cls, definitions, metadata, lock):
        """Wire the registry to multiprocessing-safe shared lists/dicts for worker processes.

        After this, ``get`` / ``all`` read definitions from *definitions* and *metadata*
        under *lock* instead of only local process state.
        """
        with cls._lock:
            cls._shared_definitions = definitions
            cls._shared_metadata = metadata
            cls._shared_lock = lock

    @classmethod
    def _using_shared_state(cls):
        """Return True if cross-process shared definitions are active."""
        return (
            cls._shared_definitions is not None
            and cls._shared_metadata is not None
            and cls._shared_lock is not None
        )

    @classmethod
    def _get_shared_definition(cls, name: str):
        """Return a deep copy of the store definition for *name*, or None."""
        with cls._shared_lock:
            for store_def in cls._shared_definitions:
                if store_def.get("Name") == name:
                    return copy.deepcopy(store_def)
        return None

    @classmethod
    def _get_shared_all_definitions(cls):
        """Return deep copies of all definitions from shared state."""
        with cls._shared_lock:
            return [copy.deepcopy(item) for item in cls._shared_definitions]

    @classmethod
    def _get_shared_metadata_snapshot(cls):
        """Return a shallow copy of metadata dicts under the shared lock."""
        with cls._shared_lock:
            return {
                key: dict(value) for key, value in dict(cls._shared_metadata).items()
            }

    @classmethod
    def _build_instance_from_definition(cls, store_def):
        """Instantiate the store class for *store_def* (Name, Type, Config).

        Raises:
            ValueError: if Type is missing.
            ImportError: if the store class cannot be resolved.
            RuntimeError: if instantiation fails.
        """
        name = store_def["Name"]
        conf = store_def["Config"]
        stype = store_def["Type"]
        if not stype:
            raise ValueError(f"Store '{name}' is missing a 'Type' field")
        try:
            StoreClass = cls._resolve_store_class(stype)
        except Exception as e:
            raise ImportError(
                f"[ERROR] Failed to resolve store '{name}' using type '{stype}': {e}"
            ) from e
        try:
            return StoreClass(conf)
        except Exception as e:
            raise RuntimeError(
                f"[ERROR] Failed to instantiate store '{name}' (type '{stype}'): {e}"
            ) from e

    @classmethod
    def _build_instances_from_definitions(cls, definitions):
        """Map each definition's Name to a new store instance."""
        instances = {}
        for store_def in definitions:
            name = store_def["Name"]
            instances[name] = cls._build_instance_from_definition(store_def)
        return instances

    @staticmethod
    def _default_metadata(definitions):
        """Build minimal registry metadata for local definitions."""
        return {
            item["Name"]: {"source": "local", "display_name": item["Name"]}
            for item in definitions
        }

    @classmethod
    def load(cls):
        """Load and instantiate all stores from configuration."""
        with cls._lock:
            if cls._loaded:
                return dict(cls._instances)

            configs = load_store_config()  # expected: {name: {type: "...", ...}}
            if not isinstance(configs, list):
                raise ValueError(
                    "load_store_config() must return a list of store configs"
                )
            cls._instances = cls._build_instances_from_definitions(configs)
            cls._definitions = [copy.deepcopy(item) for item in configs]
            cls._metadata = cls._default_metadata(configs)

            cls._loaded = True
            logger.info(
                "Loaded %d store(s): %s",
                len(cls._instances),
                ", ".join(cls._instances.keys()),
            )
            return dict(cls._instances)

    @classmethod
    def replace_from_definitions_best_effort(cls, definitions, metadata=None):
        """Replace the registry from *definitions*, skipping entries that fail to build.

        Invalid definitions are logged and omitted; metadata is filtered to valid names.
        *metadata* may supply ``source`` / display names etc.; otherwise defaults apply.
        """
        with cls._lock:
            instances = {}
            valid_names = set()
            accepted_definitions = []

            for store_def in definitions:
                name = store_def.get("Name")
                try:
                    built = cls._build_instances_from_definitions([store_def])
                    instances.update(built)
                    valid_names.add(name)
                    accepted_definitions.append(copy.deepcopy(store_def))
                except Exception as exc:
                    logger.error("Skipping invalid store '%s': %s", name, exc)

            base_metadata = metadata or cls._default_metadata(definitions)
            cls._definitions = accepted_definitions
            cls._instances = instances
            cls._metadata = {
                key: dict(value)
                for key, value in base_metadata.items()
                if key in valid_names
            }
            cls._loaded = True
            logger.info(
                "Reloaded %d valid store(s): %s",
                len(cls._instances),
                ", ".join(cls._instances.keys()),
            )
            return dict(cls._instances)

    @classmethod
    def get(cls, name: str):
        """Return the store instance named *name*.

        Loads config on first use when not using shared state. In shared mode,
        builds from the current shared definition for *name*.

        Raises:
            UnknownStoreError: if *name* is unknown (shared mode) or missing from instances.
        """
        with cls._lock:
            if cls._using_shared_state():
                store_def = cls._get_shared_definition(name)
                if store_def is None:
                    raise UnknownStoreError(name)
                instance = cls._build_instance_from_definition(store_def)
                cls._instances[name] = instance
                return instance
            if not cls._loaded:
                cls.load()
            if name not in cls._instances:
                raise UnknownStoreError(name)
            return cls._instances[name]

    @classmethod
    def all(cls):
        """Return a dict of all store names to instances.

        Triggers ``load`` when needed. In shared mode, rebuilds instances from
        the shared definition list.
        """
        with cls._lock:
            if cls._using_shared_state():
                instances = {}
                for store_def in cls._get_shared_all_definitions():
                    name = store_def["Name"]
                    instances[name] = cls._build_instance_from_definition(store_def)
                cls._instances = instances
                return dict(cls._instances)
            if not cls._loaded:
                cls.load()
            return dict(cls._instances)

    @classmethod
    def snapshot_metadata(cls):
        """Return a copy of per-store metadata (source, display_name, health flags, ...)."""
        with cls._lock:
            if cls._using_shared_state():
                return cls._get_shared_metadata_snapshot()
            if not cls._loaded:
                cls.load()
            return {key: dict(value) for key, value in cls._metadata.items()}

    @classmethod
    def export_snapshot(cls):
        """Return picklable registry data for worker process bootstrap."""
        with cls._lock:
            if not cls._loaded:
                cls.load()
            return [copy.deepcopy(item) for item in cls._definitions], {
                key: dict(value) for key, value in cls._metadata.items()
            }

    @classmethod
    def get_store_from_filetoken(cls, filetoken: str) -> FileStore:
        """Return the FileStore instance referenced by a file token.

        File tokens must have the format: "<protocol>://<store_name>/<filepath>".

        - <protocol>: identifies a known FileStore protocol (e.g. "sftp", "s3")
        - <store_name>: name of the store as configured in config.json
        - <filepath>: path within the file store

        Raises:
            ValueError: if the token format is invalid,
                        if the protocol does not correspond to any known
                        FileStore protocol,
                        if the resolved store does not exist,
                        or if the resolved store is not a FileStore.
        """
        parsed = urlparse(filetoken)
        scheme = parsed.scheme
        store_name = parsed.netloc
        path = parsed.path

        # Basic structural validation
        if not (scheme and store_name and path and path not in ("/", "")):
            raise ValueError(
                "Invalid filetoken format: expected "
                "'<protocol>://<store_name>/<filepath>'"
            )

        # Ensure stores are loaded and get the current registry snapshot
        stores = cls.all()

        # Look up the store by name
        try:
            store = stores[store_name]
        except KeyError:
            raise ValueError(f"Store '{store_name}' not found in registry") from None

        # Store must be a FileStore
        if not isinstance(store, FileStore):
            raise ValueError(f"Store '{store_name}' is not a FileStore")

        # Discover known FileStore protocols from loaded instances
        file_store_protocols = set()
        for instance in stores.values():
            if isinstance(instance, FileStore):
                module_name = instance.__class__.__module__.rsplit(".", 1)[-1]
                file_store_protocols.add(module_name)

        if scheme not in file_store_protocols:
            raise ValueError(
                f"Protocol '{scheme}' does not correspond to a known FileStore"
            )

        # Optional: ensure the token protocol matches the resolved store's protocol
        store_module_protocol = store.__class__.__module__.rsplit(".", 1)[-1]
        if store_module_protocol != scheme:
            raise ValueError(
                f"Protocol '{scheme}' does not match FileStore for '{store_name}'"
            )

        return store

    # --------------------------
    # Internal helpers
    # --------------------------

    @staticmethod
    def _resolve_store_class(type_spec: str):
        """Return the class object for a store type.

        Accepts:
          - 'name'
          - 'package.module'
          - 'package.module:Class'
        """
        # Parse optional explicit class
        if ":" in type_spec:
            module_path, class_name = type_spec.split(":", 1)
        else:
            module_path, class_name = type_spec, "Store"

        candidates = []

        def add_candidate(path: str):
            if path not in candidates:
                candidates.append(path)

        # Case A: user gave a fully-qualified module path
        if "." in module_path:
            add_candidate(module_path)
        else:
            # Case B: bare type name -> search in known bases
            add_candidate(f"stores.{module_path}")  # user project first
            add_candidate(f"{LIB_STORES_PKG}.{module_path}")  # library

            # Optional extra bases via env
            extra_bases = os.environ.get("bevault_workers_STORE_PATHS", "")
            for base in [p.strip() for p in extra_bases.split(",") if p.strip()]:
                add_candidate(f"{base}.{module_path}")

        # Case C: entry points
        try:
            eps = entry_points().select(group="bevault_workers.stores")
        except Exception:
            eps = []

        for ep in eps:
            if ep.name == module_path or ep.name == type_spec:
                obj = ep.load()
                # Entry point can target a class directly or a module exposing the class
                if isinstance(obj, type):
                    return obj
                if hasattr(obj, class_name):
                    return getattr(obj, class_name)
                raise ImportError(
                    f"Entry point '{ep.name}' loaded object '{obj}' but could not find class '{class_name}'."
                )

        # Try all candidates
        errors = []
        for mod_path in candidates:
            try:
                mod = importlib.import_module(mod_path)
                try:
                    return getattr(mod, class_name)
                except AttributeError:
                    raise ImportError(
                        f"Module '{mod_path}' does not define a '{class_name}' class."
                    )
            except Exception as e:
                errors.append(f"{mod_path}: {e}")

        raise ImportError(
            "Could not locate store class for type "
            f"'{type_spec}'. Tried: " + "; ".join(errors)
        )
