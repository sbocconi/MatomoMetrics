
from mariadbconn import MariaDBConn
from tqdm import tqdm
import traceback
import datetime
import plotly.graph_objects as go

from debugout import DebugLevels, debugout, set_dbglevel

from mtm_visit import Visit
from mtm_visitor import Visitor
from mtm_action import ActionItem

STARTING_POINTS = ['LOGIN', 'HOME', 'REGISTER','PRIVACYPOLICY','PASSWORD', 'MARKETPLACE']

class NoRealVisitor(Exception):
    pass

def main(user, password, host, port, socket, database):
    # Connect to MariaDB
    db = None
    
    
    try:
        db = MariaDBConn(user=user, database=database, host=host, socket=socket, port=port)
        # breakpoint()
        db.connect(password=password)

        ActionItem.init(db)
        Visit.init(db)
        Visitor.init(db)
        
        Testers = []
        Hackers = []
        Act_Visitors = []
        Act_Miss_Visitors = []
        Inact_Visitors = []

        for vstr_key in Visitor.Visitors:
            visitor = Visitor.Visitors[vstr_key]
            has_visited = False
            try:
                for visit in visitor.visits:
                    if len(visit.actions) == 0:
                        continue
                    all_visits = True
                    at_least_one_visit = False
                    for action in visit.actions:
                        if action.label in ["TESTING","LOCALTESTING"]:
                            raise NoRealVisitor('Tester')
                        elif action.label in ["HACK"]:
                            raise NoRealVisitor('Hacker')
                        elif action.label == None:
                            raise Exception(f'Action {action.idlink_va} in visit {visit.idvisit} has null Label')
                        elif action.label == 'VISIT':
                            at_least_one_visit = True
                        elif action.label != 'VISIT':
                            # breakpoint()
                            debugout(f'Action label: {action.label}, url: {action.url.name}', DebugLevels.VRBS)
                            all_visits = False
                    # debugout(f'All actions are visits: {all_visits}, at least one visit: {at_least_one_visit}', DebugLevels.VRBS)
                    if len(visit.actions) and at_least_one_visit:
                        has_visited = True
                if has_visited:
                    if visitor.missing_visits:
                        Act_Miss_Visitors.append(visitor)
                    else:
                        Act_Visitors.append(visitor)
                else:
                    Inact_Visitors.append(visitor)
            except NoRealVisitor as e:
                # breakpoint()
                if str(e) == 'Tester':
                    Testers.append(visitor)
                elif str(e) == 'Hacker':
                    Hackers.append(visitor)
                
        print(f'Active visitors {len(Act_Visitors)}, active visitors missing visits {len(Act_Miss_Visitors)}, inactive visitors {len(Inact_Visitors)}, testers {len(Testers)}, hackers {len(Hackers)}')        


        strange_first_visits = 0
        Normal_Visitors = []
        for visitor in Act_Visitors:
            first_visit = True
            for visit_nr, visit in enumerate(visitor.visits):
                if len(visit.actions) == 0:
                    continue
                for action in visit.actions:
                    if action.label == 'VISIT':
                        if first_visit:
                            if action.sublabel not in STARTING_POINTS:
                                strange_first_visits += 1
                                debugout(f"Strange first visit: {action.sublabel}", DebugLevels.VRBS)
                            else:
                                Normal_Visitors.append(visitor)
                            visitor.set_start_page(action.sublabel, action.server_time, visit_nr)
                        else:
                            visitor.set_reached_page(action.sublabel, action.server_time, visit_nr)
                        first_visit = False
        
        if strange_first_visits > 0:
            debugout(f'{strange_first_visits} visitors not starting from {STARTING_POINTS}', DebugLevels.WRNG)

        # Calculate average times to endpoints
        for path in ActionItem.PATH_PATTERNS:
            endpoint = path['label']
            reached_by = 0
            total_abs_time_to_page = datetime.timedelta(seconds=0)
            total_rel_time_to_page = datetime.timedelta(seconds=0)
            # The following can iterate on Normal_Visitors or on Act_Visitors
            for visitor in Normal_Visitors:
                abs_time_to_page, rel_time_to_page = visitor.time_to_endpoint(endpoint)
                if abs_time_to_page != -1:
                    reached_by += 1
                    total_abs_time_to_page +=  abs_time_to_page
                    total_rel_time_to_page +=  rel_time_to_page

            if reached_by > 0:
                abs_elapsed = total_abs_time_to_page/reached_by
                rel_elapsed = total_rel_time_to_page/reached_by
                print(f'Page {endpoint} reached by {round(reached_by/len(Act_Visitors)*100,2)}% in on average {abs_elapsed.days} days and {round(abs_elapsed.seconds/60/60,2)} hours abs, {round(rel_elapsed.total_seconds()/60,2)} mins rel')
            else:
                print(f'Page {endpoint} not reached')

        OUT = 'OUT'
        IN = 'IN'
        not_meaningful = ['ADMIN', 'API']
        all_paths = {}

        for visitor in Normal_Visitors:
            for visit_nr, visit in enumerate(visitor.visits):
                if len(visit.actions) == 0:
                    continue
                path = [IN]
                for action in visit.actions:
                    if action.label == 'VISIT':
                        # Skip staying on the same page
                        if path != [] and path[-1] == action.sublabel:
                            continue
                        if action.sublabel in not_meaningful:
                            continue
                        path.append(action.sublabel)
                if path == [IN]:
                    continue
                path.append(OUT)
                # breakpoint()
                # print('->'.join(path))

                for i in range(len(path)-1):
                    # if path != 'LOGIN' and i == 0:
                    key = f'{path[i]}_{path[i+1]}'
                    if key in all_paths:
                        all_paths[key] += 1
                    else:
                        all_paths[key] = 1


        sources = []
        targets = []
        values = []
        labels = [IN]
        labels.extend([ path['label'] for path in ActionItem.PATH_PATTERNS])
        # labels.append(IN)
        labels.append(OUT)
        # breakpoint()
        # 'IN'
        # 'REGISTER', 'LOGIN', 'HOME', 'PASSWORD', 'PROFILE', 
        # 'CONTENT', 'TEAM', 'ORGANISATION', 
        # 'MARKETPLACE', 'WISHLIST', 'PORTFOLIO', 'MODERATION', 
        # 'DAO', 
        # 'TOOLS', 
        # 'COMPETITIONS', 
        # 'FAQ', 'PRIVACYPOLICY', 
        # 'ADMIN', 'API', 
        # 'OUT'
        x = [0.1, 0.2,0.2,0.2,0.2,0.2, 0.3,0.3,0.3, 0.4,0.4,0.4,0.4, 0.5, 0.6, 0.7, 0.7,0.7, 0.7,0.7, 0.9]
        y = [0.5, 0.1,0.2,0.5,0.7,0.9, 0.3,0.5,0.7, 0.2,0.4,0.6,0.8, 0.5, 0.5, 0.1, 0.3,0.5, 0.7,0.9, 0.5]

        for key, value in all_paths.items():
            source_str, target_str = key.split('_')
            source = [i for i in range(len(labels)) if labels[i] == source_str][0]
            target = [i for i in range(len(labels)) if labels[i] == target_str][0]
            sources.append(source)
            targets.append(target)
            values.append(value)

        
        link = dict(source = sources, target = targets, value = values)
        
        # Several options for layout, watch out that with some settings
        # the vertical bars disappear.
        # node = dict(label = labels, x=x, y=y)
        # node = dict(label = labels)
        node = dict(label=labels, pad=200, x=x)
        # node = dict(label=labels, pad=250, thickness=50)
        # node = dict(label=labels, pad=200, thickness=50)
        
        data = go.Sankey(arrangement = "snap",valueformat = "d", link = link, node=node)
        # plot
        fig = go.Figure(data)
        fig.show()
                            

        breakpoint()

            

    except Exception as e:
        print(f"{''.join(traceback.format_exception(e))}")
        
    finally:
        if db != None:
            db.close_conn()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-u', '--user',
        dest='user',
        action='store',
        required=True,
        help='specifies the username to connect with',
    )
    parser.add_argument(
        '-p', '--password',
        dest='password',
        action='store',
        required=False,
        help='specifies the user password',
    )

    parser.add_argument(
        '--host',
        dest='host',
        action='store',
        required=False,
        help='specifies the host to connect to',
    )

    parser.add_argument(
        '-l','--level',
        dest='level',
        action='store',
        default=1,
        type = int,
        required=False,
        help='specifies the level of output',
    )

    parser.add_argument(
        '--port',
        dest='port',
        action='store',
        default=3306,
        type = int,
        required=False,
        help='specifies the port to connect to',
    )
    
    parser.add_argument(
        '--db',
        dest='db',
        action='store',
        default='matomo_07_07_25',
        required=False,
        help='specifies the database to connect to',
    )

    parser.add_argument(
        '--socket',
        dest='socket',
        action='store',
        default='/tmp/mysql.sock',
        required=False,
        help='specifies the socket to connect to',
    )

    args, unknown = parser.parse_known_args()

    if len(unknown) > 0:
        debugout(f'Unknown options {unknown}', DebugLevels.ERR)
        parser.print_help()
        exit(-1)

    set_dbglevel(args.level)

    main(user=args.user, password=args.password, host=args.host, port=args.port, database=args.db, socket=args.socket)
