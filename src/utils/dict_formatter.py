def format_to_dict(columns, query):
    new_dict = {}
    for column in columns:
        new_dict[f"{column}"] = query   
    return new_dict