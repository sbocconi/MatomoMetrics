ACTIONS_QUERY = "SELECT idaction, `name`, `type`, url_prefix  FROM matomo_log_action;"


class ActionItem:
    Db_Conn = None
    ActionsItems = {}

    @classmethod
    def init(cls, db):
        cls.Db_Conn = db
        cls.get_actions()

    @classmethod
    def get_actions(cls):
        actions_query = cls.Db_Conn.run_query(ACTIONS_QUERY)
        columns = [column[0] for column in actions_query.description]
        for row in actions_query:
            ags = dict(zip(columns, row))
            action = ActionItem(**ags)
            cls.ActionsItems[f'{action.idaction}'] = action


    def __init__(self, idaction, name, type, url_prefix):
        self.idaction = idaction
        self.name = name
        self.type = type
        self.url_prefix = url_prefix
    

class Action:

    def __init__(self):
        pass


