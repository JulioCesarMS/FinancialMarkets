

def load_stock_data(db, data, table_name):

    if data.empty:
        return
    # carga a base de datos
    db.insert_to_db(data, tabla=table_name, batch_size=500)