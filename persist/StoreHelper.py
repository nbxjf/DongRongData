from DbHelper import *


# data is a dict
def store_data(company_id, site, crawl_time, data, data_table):
    items = fetchmany(" SELECT key_desc, value from " + data_table + " WHERE company_id = %s AND site = %s ", data=(company_id, site))
    exist_data = dict(zip(map(lambda x: x['key_desc'], items), map(lambda x: x['value'], items)))

    insert_sql = "INSERT INTO `" + data_table + "` (`company_id`, `site`, `crawl_time`, `key_desc`, `value`) VALUES(%s, %s, %s, %s, %s)"
    update_sql = "UPDATE `" + data_table + "` SET value = %s where `key_desc`= %s AND company_id = %s AND site = %s"
    insert_data = []
    update_data = []
    exist_data_keys = exist_data.keys()
    for key_desc, value in data.items():
        insert_temp = []
        update_temp = []
        if key_desc not in exist_data_keys:
            insert_temp.append(company_id)
            insert_temp.append(site)
            insert_temp.append(crawl_time)
            insert_temp.append(key_desc)
            insert_temp.append(value)
            insert_data.append(insert_temp)
        else:
            if value != exist_data[key_desc]:
                update_temp.append(value)
                update_temp.append(key_desc)
                update_temp.append(company_id)
                update_temp.append(site)
                update_data.append(update_temp)
    executemany(insert_sql, tuple(insert_data))
    executemany(update_sql, tuple(update_data))
