from bevault_workers import WorkerManager

def main():
    """Main entry point for the worker application"""
    try:
        # Initialize the worker manager with configuration
        manager = WorkerManager(
            config_path="config.json",
            workers_module="workers"  # Discover workers from the 'workers' module
        )
        
        # Start the worker manager (this will block until stopped)
        manager.start()
        
    except KeyboardInterrupt:
        print("Application interrupted by user")
    except Exception as e:
        print(f"Application error: {e}")
        raise

if __name__ == "__main__":
    main()
