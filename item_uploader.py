import concurrent.futures
import json
import pickle
import traceback


from itemadapter import ItemAdapter


from scraper_schedule_of_classes.db.db import DataAccess


def loadall(fn):
    items = []
    with open(fn, 'rb') as items_f:
        while True:
            try:
                items.append(pickle.load(items_f))
            except EOFError:
                break
    return items


# Single thread item saver.
def save_items(items):

    conn = DataAccess.get_conn()

    for item in items:
        try:
            with conn:
                DataAccess.insert_section_group_all_info(conn, ItemAdapter(item))
        except Exception as e:
            print(f'could not insert:')
            print(item)
            print(e)
            traceback.print_exc()
    
    DataAccess.put_conn(conn)


if __name__ == '__main__':
    items = loadall('items.pickle')

    conn = DataAccess.get_conn()
    with conn:
        DataAccess.reset_for_scrape(conn)
    DataAccess.put_conn(conn)
    
    chunk_size = int(len(items) / 20)
    futures = []
    i = 0
    with concurrent.futures.ThreadPoolExecutor(20) as exec:
        # Dispatch all threads
        while i < len(items):
            chunk = items[i:i+chunk_size]
            new_future = exec.submit(save_items, chunk)
            futures.append(new_future)
            i += chunk_size
        for i, future in enumerate(concurrent.futures.as_completed(futures)):
            future.result()

    DataAccess.close()