
import datetime
import re
from urllib.parse import urlparse

from debugout import DebugLevels, debugout

from mtm_action import ActionItem, Action

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
            debugout(f"Simultaneous visit: {idvisitor} at {visit_first_action_time}, idvisit {idvisit}", DebugLevels.VRBS)
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
            url_ref = ActionItem.retrieve_entry(ags['idaction_url_ref'])
            name_ref = ActionItem.retrieve_entry(ags['idaction_name_ref'])
            url = ActionItem.retrieve_entry(ags['idaction_url'])
            name = ActionItem.retrieve_entry(ags['idaction_name'])

            if row_nr == 0:
                if not self.visit_first_action_time == ags['server_time']:
                    raise Exception(f'Visit time not consistent with first action time')
                if not (url_ref == None and name_ref == None):
                    raise Exception(f'First action cannot have previous action')
                if url != None and url.type == 'TYPE_OUTLINK':
                    debugout(f'First action is an outlink for visit {self.idvisit}', DebugLevels.VRBS)
            if ags['pageview_position'] == actions_query.rowcount:
                if not self.visit_last_action_time == ags['server_time']:
                    raise Exception(f'Visit time not consistent with last action time')
            
            if url == None:
                # This happens but it is not clear why
                debugout(f'Url is none for action nr {row_nr+1} in visit {self.idvisit}', DebugLevels.VRBS)
                continue
            
            action = Action(url=url, name=name, url_ref=url_ref, name_ref=name_ref,idlink_va=ags['idlink_va'],pageview_position=ags['pageview_position'],server_time=ags['server_time'])
            self.actions.append(action)

            if url.type == 'TYPE_PAGE_URL' or url.type == 'TYPE_EVENT_ACTION':
                
                full_url = url.url_prefix + url.name if url.url_prefix != None else url.name
                parsed_url = urlparse(full_url)
                # breakpoint()
                # print(parsed_url.scheme)
                if not parsed_url.scheme in ActionItem.ALLOWED_SCHEMES:
                    action.set_label('HACK')
                    continue
                loc_found = 0
                for loc_pattern in ActionItem.LOC_PATTERNS:
                    p = re.compile(loc_pattern['string'],re.I)
                    m = p.match(parsed_url.netloc)
                    if m:
                        loc_found += 1
                        if loc_pattern['label'] == "VISIT":
                            path_found = 0
                            
                            for path_pattern in ActionItem.PATH_PATTERNS:
                                pp = re.compile(path_pattern['string'])
                                mm = pp.match(parsed_url.path)
                                if mm:
                                   path_found += 1
                                   action.set_label(label='VISIT', sublabel=path_pattern['label'])
                                #    break
                            if path_found == 0:
                                hack_found = False
                                for hack in ActionItem.HACK_PATTERNS:
                                    # case-insensitive check
                                    if hack.lower() in parsed_url.path.lower():
                                        hack_found = True
                                        action.set_label(label='HACK')
                                        break
                                if not hack_found:
                                    debugout(f'Path not found: {parsed_url.path}', DebugLevels.WRNG)
                                    action.set_label(label='UNDEFINED')
                                # breakpoint()
                            if path_found > 1:
                                raise Exception(f'pattern found nr {path_found} for {parsed_url.path}')
                        else:
                            action.set_label(label=loc_pattern['label'])
                if not loc_found == 1:
                    raise Exception(f'pattern found nr {loc_found} for {parsed_url.netloc}')
                    # breakpoint()
            elif url.type == 'TYPE_OUTLINK':
                action.set_label('OUTLINK')
            elif url.type == 'TYPE_DOWNLOAD':
                action.set_label('DOWNLOAD')
            # elif url.type == 'TYPE_EVENT_ACTION':
            #     print(url.name)
            #     breakpoint()
            #     action.set_label('TYPE_EVENT_ACTION')
            else:
                raise Exception(f'Type unknown: {url.type}')
                # breakpoint()