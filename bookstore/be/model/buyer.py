import uuid
import json
import logging
from be.model import db_conn
from be.model import error
import pymongo


class Buyer(db_conn.DBConn):
    def __init__(self):
        db_conn.DBConn.__init__(self)

    def new_order(
        self, user_id: str, store_id: str, id_and_count: [(str, int)]
    ) -> (int, str, str):
        order_id = ""
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id) + (order_id,)
            if not self.store_id_exist(store_id):
                return error.error_non_exist_store_id(store_id) + (order_id,)
            uid = "{}_{}_{}".format(user_id, store_id, str(uuid.uuid1()))

            for book_id, count in id_and_count:
                book = self.conn.store.find_one({"store_id": store_id, "book_id": book_id})
                if book is None:
                    return error.error_non_exist_book_id(book_id) + (order_id,)

                stock_level = book.get("stock_level", 0)
                book_info = book.get("book_info", "{}")
                book_info_json = json.loads(book_info)
                price = book_info_json.get("price")

                if stock_level < count:
                    return error.error_stock_level_low(book_id) + (order_id,)

                cursor = self.conn.store.update_one({
                    "store_id": store_id,
                    "book_id": book_id,
                    "stock_level": {"$gte": count}},
                    {"$inc": {"stock_level": -count}}
                )
                if cursor.matched_count == 0:
                    return error.error_stock_level_low(book_id) + (order_id,)

                self.conn.new_order_detail.insert_one({
                    "order_id": uid,
                    "book_id": book_id,
                    "count": count,
                    "price": price
                })

            self.conn.new_order.insert_one({
                "order_id": uid,
                "store_id": store_id,
                "user_id": user_id
            })
            order_id = uid
        except pymongo.errors.PymongoError as e:
            logging.info("528, {}".format(str(e)))
            return 528, "{}".format(str(e)), ""
        except BaseException as e:
            logging.info("530, {}".format(str(e)))
            return 530, "{}".format(str(e)), ""

        return 200, "ok", order_id

    def payment(self, user_id: str, password: str, order_id: str) -> (int, str):
        conn = self.conn
        try:
            order = conn.new_order.find_one({"order_id": order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)
            
            buyer_id = order["user_id"]
            store_id = order["store_id"]

            if buyer_id != user_id:
                return error.error_authorization_fail()

            buyer = conn.user.find_one({"user_id": buyer_id})
            if buyer is None:
                return error.error_non_exist_user_id(buyer_id)
            balance = buyer["balance"]
            if password != buyer["password"]:
                return error.error_authorization_fail()

            store = conn.user_store.find_one({"store_id": store_id})
            if store is None:
                return error.error_non_exist_store_id(store_id)

            seller_id = store["user_id"]

            if not self.user_id_exist(seller_id):
                return error.error_non_exist_user_id(seller_id)

            order_details = conn.new_order_detail.find({"order_id": order_id})
            total_price = 0
            for detail in order_details:
                count = detail["count"]
                price = detail["price"]
                total_price += price * count

            if balance < total_price:
                return error.error_not_sufficient_funds(order_id)

            cursor = conn.user.update_one({
                "user_id": buyer_id,
                "balance": {"$gte": total_price}},
                {"$inc": {"balance": -total_price}}
            )
            if cursor.matched_count == 0:
                return error.error_not_sufficient_funds(order_id)

            cursor = conn.user.update_one(
                {"user_id": seller_id},
                {"$inc": {"balance": total_price}}
            )
            if cursor.matched_count == 0:
                return error.error_non_exist_user_id(seller_id)

            conn.new_order.delete_one({"order_id": order_id})

            conn.new_order_detail.delete_many({"order_id": order_id})

        except pymongo.errors.PymongoError as e:
            return 528, "{}".format(str(e))

        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"

    def add_funds(self, user_id, password, add_value) -> (int, str):
        try:
            row = self.conn.user.find_one({"user_id": user_id})
            if row is None:
                return error.error_authorization_fail()

            if row["password"] != password:
                return error.error_authorization_fail()

            cursor = self.conn.user.update_one(
            {"user_id": user_id},
            {"$inc": {"balance": add_value}}
            )
            if cursor.matched_count == 0:
                return error.error_non_exist_user_id(user_id)
        except pymongo.errors.PymongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))

        return 200, "ok"
    
    def order_receive(self, user_id: str, order_id: str) -> (int, str):
        try:
            if not self.user_id_exist(user_id):
                return error.error_non_exist_user_id(user_id)

            order = self.conn.order_history.find_one({'order_id': order_id})
            if order is None:
                return error.error_invalid_order_id(order_id)
            
            status = order['status']
            if status != 'express':
                return error.error_not_express(order_id)
            
            self.conn.order_history.update_one(
                {'order_id': order_id},
                {'$set': {'status': 'received'}},
            )
        except pymongo.errors.PyMongoError as e:
            return 528, "{}".format(str(e))
        except BaseException as e:
            return 530, "{}".format(str(e))
        return 200, "ok"