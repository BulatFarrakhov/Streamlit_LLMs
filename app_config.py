class AppConfig:
    db = ""
    schema = ""
    table = ""

    @classmethod
    def set_config(cls, db, schema, table):
        cls.db = db
        cls.schema = schema
        cls.table = table
