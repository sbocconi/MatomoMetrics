import datetime

from mtm_visit import Visit

# Ideally this is the user id, but it must be set
# USER_ID = 'user_id'
USER_ID = 'idvisitor'
USER_FILTER = 'profilable = 1'
USERS_QUERY = f"SELECT DISTINCT HEX({USER_ID}) FROM matomo_log_visit WHERE {USER_FILTER};"

MISSING_VISITS_USERS_QUERY = 'SELECT HEX(idvisitor) FROM matomo_log_visit GROUP BY idvisitor HAVING MIN(visitor_returning) > 0'
VISITS_QUERY = "" \
"SELECT idvisit, visitor_localtime, visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first, " \
"visit_first_action_time,visit_last_action_time, visit_exit_idaction_url, visit_exit_idaction_name, visit_entry_idaction_url, " \
"visit_entry_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_total_time, visit_goal_converted " \
"FROM matomo_log_visit WHERE {}=UNHEX('{}');"


class Visitor:
    Db_Conn = None
    Missing_Visits_Users = []
    Visitors = {}

    def __init__(self, user_id, missing_visits=False):
        self.user_id = user_id
        self.missing_visits = missing_visits
        self.visits_to_add = 0
        self.sim_visits = 0
        self.visits = {}
        self.first_visit = None
        self.last_visit = None
        self.one_bl_visit = None
        self.two_bl_visit = None
        
    @classmethod
    def init(cls, db):
        cls.Db_Conn = db
        cls.users_with_missing_visits()
        cls.get_users()
        cls.get_visits()

    @classmethod
    def users_with_missing_visits(cls):
        
        missing_visits_users_query = cls.Db_Conn.run_query(MISSING_VISITS_USERS_QUERY)

        for idvisitor in missing_visits_users_query:
            # breakpoint()
            cls.Missing_Visits_Users.append(idvisitor[0])

        print(f'Nr users missing visits: {len(cls.Missing_Visits_Users)}')

    @classmethod
    def get_users(cls):
        users_query = cls.Db_Conn.run_query(USERS_QUERY)
        for id_arr in users_query:
            id = id_arr[0]
            if id in cls.Visitors:
                raise Exception(f'User {id} already processed')
            if id in cls.Missing_Visits_Users:
                cls.Visitors[id] = Visitor(id, missing_visits=True)
            else:
                cls.Visitors[id] = Visitor(id)

    @classmethod
    def get_visits(cls):
        for key in cls.Visitors:
            visitor = cls.Visitors[key]
            visits_query = cls.Db_Conn.run_query(VISITS_QUERY.format(*[USER_ID,visitor.user_id]))
            columns = [column[0] for column in visits_query.description]
            for row in visits_query:
                ags = dict(zip(columns, row))
                if not visitor.add_visit(**ags):
                    raise Exception(f"Failed adding visit {ags['idvisit']} to user {visitor.user_id}")


    def add_visit(self, idvisit, visitor_localtime, visit_first_action_time, visit_last_action_time, visit_total_time, visit_entry_idaction_url, 
                 visit_entry_idaction_name, visit_exit_idaction_url, visit_exit_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_goal_converted,
                 visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first):
        # breakpoint()
        if f'{idvisit}' in self.visits:
            raise Exception(f'Visit {idvisit} already exists!')

        visit = Visit(idvisit, visitor_localtime, visit_first_action_time, visit_last_action_time, visit_total_time, visit_entry_idaction_url, 
                 visit_entry_idaction_name, visit_exit_idaction_url, visit_exit_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_goal_converted)
        self.visits[f'{idvisit}'] = visit
        
        if self.first_visit == None:
            self.first_visit = visit
        if self.last_visit != None:
            self.two_bl_visit = self.one_bl_visit
            self.one_bl_visit = self.last_visit
        self.last_visit = visit

        if not self.check(visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first):
            return False
        return True
        
    def check(self, visitor_returning:int, count_visits:int, seconds_since_last:int, seconds_since_first:int) -> bool:
        # This needs to be the first check as it detects and adjust for simultaneous visits and missing visits
        if not self.check_count(count_visits,seconds_since_first):
            print(f'{self.user_id}: count_visits {count_visits} differs from recorded {self.get_nr_visits()}')
            return False

        if (visitor_returning and self.get_nr_visits() < 2) or (not visitor_returning and self.get_nr_visits() > 1):
            print(f'{self.user_id}: visitor_returning {visitor_returning} differs from recorded visit length: {self.get_nr_visits()}')
            # breakpoint()
            return False
        # breakpoint()
        # Check seconds from last visit if we have them but not if we miss visit and this is the first one we recorded
        if seconds_since_last > 0 and not (self.missing_visits and self.one_bl_visit == None):
            rec_secs = (self.last_visit.visit_first_action_time - self.one_bl_visit.visit_first_action_time).total_seconds()
            if seconds_since_last != rec_secs:
                print(f'{self.user_id}: seconds_since_last {seconds_since_last} differs from recorded {rec_secs}')
                # breakpoint()
                return False
        if seconds_since_first > 0:
            rec_secs = (self.last_visit.visit_first_action_time - self.first_visit.visit_first_action_time).total_seconds()
            if seconds_since_first != rec_secs:
                print(f'{self.user_id}: seconds_since_first {seconds_since_first} differs from recorded {rec_secs}')
                # breakpoint()
                return False
        return True

    def check_count(self, count_visits, seconds_since_first):
        if count_visits != self.get_nr_visits():
            if Visit.is_simultaneous_visit(self.user_id, self.last_visit):
                self.sim_visits += 1
                self.one_bl_visit = self.two_bl_visit
                if count_visits == self.get_nr_visits():
                    return True
                else:
                    return False
            elif count_visits > self.get_nr_visits() and self.missing_visits:
                self.add_fake_first_visit(seconds_since_first)
                self.visits_to_add = count_visits - self.get_nr_visits()
                if count_visits == self.get_nr_visits():
                    return True
                else:
                    return False
            else:
                return False
        return True
    
    def get_nr_visits(self):
        return len(self.visits) - self.sim_visits + self.visits_to_add
        
    def add_fake_first_visit(self, seconds_since_first):
        visit_first_action_time = self.last_visit.visit_first_action_time - datetime.timedelta(seconds=seconds_since_first)
        fake_visit = Visit.create_fake_first_visit(visit_first_action_time)
        self.first_visit = fake_visit






