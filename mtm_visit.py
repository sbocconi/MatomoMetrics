
import datetime

SIM_VISITS_QUERY = '''SELECT HEX(idvisitor), visit_first_action_time, idvisit 
                        FROM matomo_log_visit
                        WHERE visit_first_action_time IN (SELECT visit_first_action_time
                            FROM matomo_log_visit 
                            GROUP BY HEX(idvisitor), visit_first_action_time 
                            HAVING COUNT(visit_first_action_time) > 1
                        );
            '''
VISIT_ACTION_QUERY = 'SELECT idlink_va, idaction_url_ref, idaction_name_ref, pageview_position, server_time, idpageview, idaction_name, idaction_url, time_spent_ref_action ' \
    'FROM matomo_log_link_visit_action WHERE idvisit = {} ORDER BY pageview_position, server_time ASC'

class Visit:
    Sim_Visits = {}
    Db_Conn = None

    def __init__(self, idvisit, visitor_localtime, visit_first_action_time, visit_last_action_time, visit_total_time, visit_entry_idaction_url, 
                 visit_entry_idaction_name, visit_exit_idaction_url, visit_exit_idaction_name, visit_total_actions, visit_total_searches, visit_total_events, goal_converted):
        self.idvisit = idvisit
        self.visitor_localtime = visitor_localtime
        self.visit_first_action_time = visit_first_action_time
        self.visit_last_action_time = visit_last_action_time
        self.visit_total_time = visit_total_time
        self.visit_exit_idaction_url = visit_exit_idaction_url
        self.visit_exit_idaction_name = visit_exit_idaction_name
        self.visit_entry_idaction_url = visit_entry_idaction_url
        self.visit_entry_idaction_name = visit_entry_idaction_name
        self.visit_total_actions = visit_total_actions
        self.visit_total_searches = visit_total_searches
        self.visit_total_events = visit_total_events
        self.goal_converted = goal_converted
        self.actions = []
        self.fetch_actions()

    @classmethod
    def init(cls, db):
        cls.Db_Conn = db
        cls.simultaneous_visits()

    @classmethod
    def simultaneous_visits(cls):
        """
            Detect simultaneous visits that do not contribute
            to the total number of visits

        """
        sim_visits_query = cls.Db_Conn.run_query(SIM_VISITS_QUERY)
        for idvisitor, visit_first_action_time, idvisit in sim_visits_query:
            print(f"Simultaneous visit: {idvisitor} at {visit_first_action_time}, idvisit {idvisit}")
            key = f'{idvisitor}_{visit_first_action_time}'
            if key in cls.Sim_Visits:
                cls.Sim_Visits[key].append(idvisit)
            else:
                cls.Sim_Visits[key] = [idvisit]
        
    @classmethod
    def is_simultaneous_visit(cls, idvisitor, visit):
        key = f'{idvisitor}_{visit.visit_first_action_time}'
        if key in cls.Sim_Visits:
            return True
        return False

    @classmethod
    def create_fake_first_visit(cls,visit_first_action_time):
        fake_visit = Visit(idvisit=-1, visitor_localtime=None, visit_first_action_time=visit_first_action_time, 
                           visit_last_action_time=visit_first_action_time, visit_total_time=0, visit_entry_idaction_url=None, 
                           visit_entry_idaction_name=None, visit_exit_idaction_url=None, visit_exit_idaction_name=None,
                            visit_total_actions=0, visit_total_searches=0, visit_total_events=0, goal_converted=0)
        return fake_visit

    def fetch_actions(self):
        actions_query = self.Db_Conn.run_query(VISIT_ACTION_QUERY.format(*[self.idvisit]))
        columns = [column[0] for column in actions_query.description]
        
        for row_nr, row in enumerate(actions_query):
            ags = dict(zip(columns, row))
            if row_nr == 0:
                if not self.visit_first_action_time == ags['server_time']:
                    breakpoint()
                    raise Exception(f'Visit time not consistent with first action time')
            if ags['pageview_position'] == actions_query.rowcount:
                if not self.visit_last_action_time == ags['server_time']:
                    breakpoint()
                    raise Exception(f'Visit time not consistent with last action time')

            # breakpoint()
