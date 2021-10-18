from os import environ
import pandas as pd
import sqlalchemy


def create_engine(database):
    if database == 'mysql':
        login = environ['mysql_login']
        password = environ['mysql_password']
        host = environ['mysql_host']
        return sqlalchemy.create_engine(f"mysql://{login}:{password}@{host}?charset=utf8")
    elif database == 'ms_db':
        login = environ['ms_db_login']
        password = environ['ms_db_password']
        host = environ['ms_db_host']
        return sqlalchemy.create_engine(f"postgresql://{login}:{password}@{host}")


def sql_requester(sql_query):
    if sql_query['title'] == 'client_data':
        client_login = sql_query['client_login']
        query = f"""
        SELECT Client.PublicClientID, 
               SUM(ClientCoupon.points_earned) points_earned
        FROM Client 
        LEFT JOIN (SELECT PublicClientID, 
                          CASE WHEN PurchaseAmount < 300 THEN 1
                               WHEN PurchaseAmount >= 300 AND
                                    PurchaseAmount < 420 THEN 2
                               WHEN PurchaseAmount >= 420 THEN 3
                          END AS points_earned
                   FROM ClientCoupon
                   WHERE PurchaseAmount >= 180
                   AND ValidatedTimestamp > '2021-10-13'
                   AND ValidatedTimestamp < '2021-11-02') 
                   AS ClientCoupon ON Client.PublicClientID = ClientCoupon.PublicClientID
        WHERE Client.PartnerID = 2896
        AND Client.Login = '{client_login}'
        GROUP BY PublicClientID"""
        engine = create_engine('mysql')
        df = pd.read_sql(query, engine)
        if df.empty:
            return
        client_id = int(df['PublicClientID'].values[0])
        points_earned = df['points_earned'].values[0]
        if points_earned:
            points_earned = int(points_earned)
        else:
            points_earned = 0

        query = f"""
        SELECT validation.promocode,
               promocode_type.promocode_type_id,
               promocode_type.price, 
               promocode_type.source,
               promocode_type.title,
               promocode_type.text
        FROM validation
        JOIN promocode_type ON validation.promocode_type_id = promocode_type.promocode_type_id
        WHERE validation.client_id = {client_id}"""
        engine = create_engine('ms_db')
        df = pd.read_sql(query, engine)

        points_spend = df['price'].sum()
        balance = points_earned - points_spend

        if df.empty:
            purchases = []
        else:
            purchases = df.apply(lambda x: {'source': x['source'],
                                            'title': x['title'],
                                            'promocode': x['promocode'],
                                            'text': x['text']}, axis=1).values.tolist()

        client_data = {'client_id': client_id,
                       'client_login': client_login,
                       'balance': balance,
                       'purchases': purchases}
        return client_data

    elif sql_query['title'] == 'offers':
        query = f"""
        SELECT DISTINCT(promocode_type.promocode_type_id),
               promocode_type.source, 
               promocode_type.title, 
               promocode_type.price
        FROM promocode_type
        JOIN validation ON promocode_type.promocode_type_id = validation.promocode_type_id
        WHERE validation.client_id IS NULL
        ORDER BY promocode_type.promocode_type_id"""
        engine = create_engine('ms_db')
        df = pd.read_sql(query, engine)
        return df

    elif sql_query['title'] == 'get_offer_price':
        promocode_type_id = sql_query['promocode_type_id']
        query = f"""
        SELECT price
        FROM promocode_type
        WHERE promocode_type_id = {promocode_type_id}"""
        engine = create_engine('ms_db')
        df = pd.read_sql(query, engine)
        offer_price = df['price'].values[0]
        return offer_price

    elif sql_query['title'] == 'validation':
        client_id = sql_query['client_id']
        promocode_type_id = sql_query['promocode_type_id']
        query = f"""
        UPDATE validation
        SET client_id = {client_id},
            validation_timestamp = NOW()
        WHERE validation_id IN (SELECT validation_id
                                FROM validation 
                                WHERE promocode_type_id = {promocode_type_id}
                                AND client_id IS NULL
                                LIMIT 1)"""
        engine = create_engine('ms_db')
        engine.execute(query)

    elif sql_query['title'] == 'read_ms_db':
        query = sql_query['query']
        engine = create_engine('ms_db')
        df = pd.read_sql(query, engine)
        for column in df.columns:
            df[column] = df[column].apply(lambda x: x if x and x == x else '')
            if column == 'validation_timestamp':
                df[column] = df[column].astype(str)
            df[column] = df[column].astype(str)
        df = df.to_dict()
        return df

    elif sql_query['title'] == 'add_promocodes':
        df = pd.DataFrame(sql_query['data'])
        engine = create_engine('ms_db')
        df.to_sql('validation', engine, if_exists='append', index=False)

    elif sql_query['title'] == 'direct_sql_query':
        query = sql_query['query']
        engine = create_engine('ms_db')
        engine.execute(sqlalchemy.text(query))

    elif sql_query['title'] in ('check_mysql_connection',
                                'check_ms_db_connection'):
        if sql_query['title'] == 'check_mysql_connection':
            query = f"""
            SELECT PublicClientID
            FROM Client
            LIMIT 1"""
            engine = create_engine('mysql')
        elif sql_query['title'] == 'check_ms_db_connection':
            query = f"""
            SELECT validation_id
            FROM validation
            LIMIT 1"""
            engine = create_engine('ms_db')
        try:
            df = pd.read_sql(query, engine)
            if not df.empty:
                result = {'status': True,
                          'message': ''}
            else:
                result = {'status': False,
                          'message': 'Successful SQL request, but answer is empty'}
        except Exception as e:
            result = {'status': False,
                      'message': str(e)}
        return result
