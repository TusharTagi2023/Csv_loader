class MultidbRouter:
    def db_for_read(self, model, **hints):
        # Return the name of the dynamic database based on your logic
        return 'dynamic_db'

    def db_for_write(self, model, **hints):
        # Return the name of the dynamic database based on your logic
        return 'dynamic_db'

    def allow_relation(self, obj1, obj2, **hints):
        # Allow relations if both objects are in the dynamic database
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        # Only allow migrations for the dynamic database
        if db == 'dynamic_db':
            return True
        return False
