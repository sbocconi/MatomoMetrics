
from matomodb import MatomoDB
from tqdm import tqdm
import traceback
import datetime

from debugout import DebugLevels, debugout, set_dbglevel

from mtm_visit import Visit
from mtm_visitor import Visitor
from mtm_action import ActionItem

STARTING_POINTS = ['LOGIN', 'HOME', 'REGISTER','PRIVACYPOLICY','PASSWORD']

class NoRealVisitor(Exception):
    pass

def main(user, password, host, port, socket, database):
    # Connect to MariaDB
    db = None
    
    
    try:
        db = MatomoDB(database, host, socket, port)
        # breakpoint()
        db.connect(user=user,password=password)

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
        for visitor in Act_Visitors:
            first_visit = True
            for visit in visitor.visits:
                    if len(visit.actions) == 0:
                        continue
                    for action in visit.actions:
                        if action.label == 'VISIT':
                            if first_visit:
                                if action.sublabel not in STARTING_POINTS:
                                    strange_first_visits += 1
                                    debugout(f"Strange first visit: {action.sublabel}", DebugLevels.VRBS)
                                visitor.set_start_page(action.sublabel, action.server_time)
                            else:
                                visitor.set_reached_page(action.sublabel, action.server_time)
                            first_visit = False
        
        if strange_first_visits > 0:
            debugout(f'{strange_first_visits} visitors not starting from {STARTING_POINTS}', DebugLevels.WRNG)

        for path in ActionItem.PATH_PATTERNS:
            endpoint = path['label']
            reached_by = 0
            total_time_to_page = datetime.timedelta(seconds=0)
            for visitor in Act_Visitors:
                time_to_page = visitor.time_to_endpoint(endpoint)
                if time_to_page != -1:
                    reached_by += 1
                    total_time_to_page +=  time_to_page
            if reached_by > 0:
                elapsed = total_time_to_page/reached_by
                print(f'Page {endpoint} reached by {round(reached_by/len(Act_Visitors)*100,2)}% in average {elapsed.days} days and {round(elapsed.seconds/60/60,2)} hours')
            else:
                print(f'Page {endpoint} not reached')


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
        default='matomo',
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
