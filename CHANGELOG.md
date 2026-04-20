# Changelog

## [0.3.0](https://github.com/depfac/bevault-workers-python/compare/v0.2.1...v0.3.0) (2026-04-20)


### Features

* **snowflake:** add Snowflake connector dependency and configuration fixture ([#13](https://github.com/depfac/bevault-workers-python/issues/13)) ([74fbeec](https://github.com/depfac/bevault-workers-python/commit/74fbeec5cc8cede9b6c35017c6902c8db691fb73))

## [0.2.1](https://github.com/depfac/bevault-workers-python/compare/v0.2.0...v0.2.1) (2026-04-14)


### Bug Fixes

* **sftp:** reconnect when SSH session is dead ([0128088](https://github.com/depfac/bevault-workers-python/commit/01280884643163609fa488c7bc22bfd8286b8ff7))
* **sftp:** reconnect when SSH session is dead ([78eeb6a](https://github.com/depfac/bevault-workers-python/commit/78eeb6a453a9090fd44d4e02f5f0be796faae034))

## [0.2.0](https://github.com/depfac/bevault-workers-python/compare/v0.1.2...v0.2.0) (2026-04-03)


### Features

* add SQL Server DbStore and dev probe worker ([c3afc4c](https://github.com/depfac/bevault-workers-python/commit/c3afc4c947a270547b5e02c8d26bcd025d50db9a))
* **store-registry:** tolerate invalid stores and optimize get_store_from_filetoken ([67e0408](https://github.com/depfac/bevault-workers-python/commit/67e04085b730d7831727b9d55c3bd095d37dc5d4))
* **store:** add support for gitlab store ([e28afe0](https://github.com/depfac/bevault-workers-python/commit/e28afe0af720efa14411c2e8ac31fcdca942768d))
* **stores:** Add support for gitlab as a readonly FileStore ([587e552](https://github.com/depfac/bevault-workers-python/commit/587e552bd4e00e13c4491207954ddebc0638c998))


### Documentation

* review README ([b6d09a5](https://github.com/depfac/bevault-workers-python/commit/b6d09a5f4f8ecf8e7491f8e5b8a1940b724e8c15))
* review README ([843daf2](https://github.com/depfac/bevault-workers-python/commit/843daf2d02b17787c52741d0ee0bdafcd5d25894))

## [0.1.2](https://github.com/depfac/bevault-workers-python/compare/v0.1.1...v0.1.2) (2026-04-01)


### Bug Fixes

* align project name with PyPi project ([05e9f27](https://github.com/depfac/bevault-workers-python/commit/05e9f277f109d0c2fc603c695eeb385d7d7b0d95))

## [0.1.1](https://github.com/depfac/bevault-workers-python/compare/v0.1.0...v0.1.1) (2026-04-01)


### Bug Fixes

* Fix deployment and publish on pypi ([6667b5a](https://github.com/depfac/bevault-workers-python/commit/6667b5acc1e2b2c789413d803c2a609a077bfdcf))


### Documentation

* **examples:** add example to use this library and create custom workers and stores ([2d134e8](https://github.com/depfac/bevault-workers-python/commit/2d134e8072fca8820f4fa290fc5a9a1b939a2d7e))

## Unreleased

### Features

* Built-in `sqlserver` DbStore (ODBC via pyodbc), with PostgreSQL-like config fields and optional `connectionString`.

## 0.1.0 (2026-03-31)


### Features

* inital project setup ([c85fc6b](https://github.com/depfac/bevault-workers-python/commit/c85fc6b35381a187951c521ca764833edd67e637))
