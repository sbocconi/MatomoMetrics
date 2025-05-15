import datetime

from debugout import DebugLevels, debugout
from mtm_visit import Visit

# Ideally this is the user id, but it must be set
# USER_ID = 'user_id'
USER_ID = 'idvisitor'
USER_FILTER = 'profilable = 1'
USERS_QUERY = f"SELECT DISTINCT HEX({USER_ID}) FROM matomo_log_visit WHERE idsite = 1 AND {USER_FILTER};"

MISSING_VISITS_USERS_QUERY = 'SELECT HEX(idvisitor) FROM matomo_log_visit WHERE idsite = 1 GROUP BY idvisitor HAVING MIN(visitor_returning) > 0'
VISITS_QUERY = "" \
"SELECT idvisit, visitor_localtime, visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first, " \
"visit_first_action_time,visit_last_action_time, visit_exit_idaction_url, visit_exit_idaction_name, visit_entry_idaction_url, " \
"visit_entry_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_total_time, visit_goal_converted " \
"FROM matomo_log_visit WHERE idsite = 1 AND {}=UNHEX('{}') ORDER BY visitor_count_visits ASC, visitor_localtime ASC;"


class Visitor:
    Db_Conn = None
    Missing_Visits_Users = []
    Visitors = {}

    def __init__(self, id, missing_visits=False):
        self.id = id
        self.missing_visits = missing_visits
        self.visits_to_add = 0
        self.sim_visits = 0
        self.visits = []
        self.first_visit = None
        self.last_visit = None
        self.one_bl_visit = None
        self.two_bl_visit = None

        self.reached_pages = {}
        
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

        debugout(f'Nr users missing visits: {len(cls.Missing_Visits_Users)}', DebugLevels.VRBS)

    @classmethod
    def get_users(cls):
        """
            Retrieve all users and set the missing_visits flag for each of them
        """
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
            visits_query = cls.Db_Conn.run_query(VISITS_QUERY.format(*[USER_ID,visitor.id]))
            # Automatically get the name of the columns
            columns = [column[0] for column in visits_query.description]
            for row in visits_query:
                # Assign each column its value
                ags = dict(zip(columns, row))
                if not visitor.add_visit(**ags):
                    raise Exception(f"Failed adding visit {ags['idvisit']} to user {visitor.id}")
                    # debugout(f"Failed adding visit {ags['idvisit']} to user {visitor.id}", DebugLevels.ERR)
                    # continue


    def add_visit(self, idvisit, visitor_localtime, visit_first_action_time, visit_last_action_time, visit_total_time, visit_entry_idaction_url, 
                 visit_entry_idaction_name, visit_exit_idaction_url, visit_exit_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_goal_converted,
                 visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first):
        # breakpoint()
        if  [ visit for visit in self.visits if visit.idvisit == idvisit ] != [] :
            raise Exception(f'Visit {idvisit} already exists!')

        visit = Visit(idvisit, visitor_localtime, visit_first_action_time, visit_last_action_time, visit_total_time, visit_entry_idaction_url, 
                 visit_entry_idaction_name, visit_exit_idaction_url, visit_exit_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, visit_goal_converted)
        self.visits.append(visit)
        
        if self.first_visit == None:
            self.first_visit = visit
        if self.last_visit != None:
            # remember previous visits in case we need to restore them for simultaneous visits
            self.two_bl_visit = self.one_bl_visit
            self.one_bl_visit = self.last_visit
        self.last_visit = visit

        if not self.check(visitor_returning, visitor_count_visits, visitor_seconds_since_last, visitor_seconds_since_first):
            return False
        return True
        
    def check(self, visitor_returning:int, count_visits:int, seconds_since_last:int, seconds_since_first:int) -> bool:
        # This needs to be the first check as it detects and adjust for simultaneous visits and missing visits
        if not self.check_fix_count(count_visits,seconds_since_first):
            debugout(f'{self.id}: count_visits {count_visits} differs from recorded {self.get_nr_visits()}', DebugLevels.ERR)
            return False

        # Check that returning visitors have more than one visit and not returning visitors have 0 visits
        if (visitor_returning and self.get_nr_visits() < 2) or (not visitor_returning and self.get_nr_visits() > 1):
            debugout(f'{self.id}: visitor_returning {visitor_returning} differs from recorded visit length: {self.get_nr_visits()}', DebugLevels.ERR)
            # breakpoint()
            return False
        # breakpoint()
        # Check seconds from last visit if we have them, but not if we miss visits and this is the first visit we record 
        # (in that case one_bl_visit is None), to avoid checking across the gap of missing visits
        if seconds_since_last > 0 and not (self.missing_visits and self.one_bl_visit == None):
            rec_secs = (self.last_visit.visit_first_action_time - self.one_bl_visit.visit_first_action_time).total_seconds()
            if seconds_since_last != rec_secs:
                debugout(f'{self.id}: seconds_since_last {seconds_since_last} differs from recorded {rec_secs}', DebugLevels.ERR)
                # breakpoint()
                return False
        # Check seconds since the first visit
        if seconds_since_first > 0:
            rec_secs = (self.last_visit.visit_first_action_time - self.first_visit.visit_first_action_time).total_seconds()
            if seconds_since_first != rec_secs:
                debugout(f'{self.id}: seconds_since_first {seconds_since_first} differs from recorded {rec_secs}', DebugLevels.ERR)
                # breakpoint()
                return False
        return True

    def check_fix_count(self, count_visits, seconds_since_first):
        """
            This function checks whether the DB-reported nr of visits
            is the same of what we have recorded. In case they differ,
            there can be 2 cases that can be fixed: the first one is 
            that there are simultaneous visits, the second that there 
            are missing visits. The function tries to fix these two cases 
            and fails otherwise.
        """
        # if count_visits == self.get_nr_visits() and Visit.is_simultaneous_visit(self.id, self.last_visit):
        #         print(f"{self.id} - {self.last_visit.idvisit}")
        #         # breakpoint()
        if count_visits != self.get_nr_visits():
            sim_visits = Visit.nr_simultaneous_visits(self.id, self.visits)
            if sim_visits > 0 :
                if Visit.is_simultaneous_visit(self.id, self.last_visit):
                    self.sim_visits += 1
                    # one before the last is simultaneous to last, so we make
                    # two before last the one before last (to comply to Matomo logic)
                    self.one_bl_visit = self.two_bl_visit
                else:
                    # There are simultaneous visits, but not the last one
                    # Adjust the number of visits, do not change anything in the 
                    # last visits order (unlike the case of simultaneous last visit)
                    self.sim_visits = sim_visits
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
        # Takes into account simultaneous visits and missing visits
        return len(self.visits) - self.sim_visits + self.visits_to_add
        
    def add_fake_first_visit(self, seconds_since_first):
        # We have a first visit but it is not in the DB, what we have is
        # just how long ago it was
        visit_first_action_time = self.last_visit.visit_first_action_time - datetime.timedelta(seconds=seconds_since_first)
        fake_visit = Visit.create_fake_first_visit(visit_first_action_time)
        self.first_visit = fake_visit

    def set_start_page(self, page:str, server_time:datetime, visit_nr:int):
        if 'START' in self.reached_pages:
            raise Exception(f'Visitor {self.id} has already a start')
        self.reached_pages['START'] = {
                                        'time': server_time,
                                        'start_point' : page,
                                        'visit_nr' : visit_nr
                                        }

    def set_reached_page(self, page:str, server_time:datetime, visit_nr:int):
        if page in self.reached_pages:
            self.reached_pages[page]['times'].append(server_time)
            self.reached_pages[page]['visit_nrs'].append(visit_nr)
        else:
            self.reached_pages[page] = {
                'times': [server_time],
                'visit_nrs' : [visit_nr],
            }

    def time_to_endpoint(self, endpoint):
        """
            This function calculates the time a user takes to reach a page,
            both absolute (time endpoint reached - time of start) and 
            relative (sum of of visits time up until reaching the endpoint)
        """
        start = self.reached_pages['START']['time']
        if endpoint == self.reached_pages['START']['start_point']:
            return datetime.timedelta(seconds=0), datetime.timedelta(seconds=0)
        if endpoint in self.reached_pages:
            # earliest time the endpoint was reached
            earliest = min(self.reached_pages[endpoint]['times'])
            # index of the visit of the earliest endpoint visit
            visit_nr_idx = [i for i in range(len(self.reached_pages[endpoint]['times'])) if self.reached_pages[endpoint]['times'][i] == earliest][0]
            # what visit nr was the earliest to reach the endpoint?
            visit_nr = self.reached_pages[endpoint]['visit_nrs'][visit_nr_idx]
            rel_time = datetime.timedelta(seconds=0)
            # calculate the time of all visit up to that visit
            for visit in self.visits[:visit_nr]:
                if visit.actions == []:
                    debugout(f'No actions for visitor {self.id} in visit {visit.idvisit} ', DebugLevels.VRBS)
                    continue
                rel_time = rel_time + visit.actions[-1].server_time - visit.actions[0].server_time
            # relative time is time of all visits up to that visit + the time of that visit up
            # until reaching the endpoint
            rel_time = rel_time + earliest - self.visits[visit_nr].actions[0].server_time
            # breakpoint()
            return earliest - start, rel_time
        return -1, -1






