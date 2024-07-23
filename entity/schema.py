class db_field:
    type_default_value = {
        "bpchar": "*",
        "varchar": "*",
        "int4": 20991231,
        "numeric": 0,
    }

    def __init__(self, col_name, col_type, col_notnull) -> None:
        self.col_name = col_name
        self.col_type = col_type
        self.col_notnull = col_notnull
        if self.col_notnull:
            self.value = None
        else:
            if col_type:
                self.value = self.type_default_value["bpchar"]
        pass
