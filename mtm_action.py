import datetime

ACTIONS_QUERY = "SELECT idaction, `name`, `type`, url_prefix  FROM matomo_log_action;"


class ActionItem:
    Db_Conn = None
    ActionsItems = {}
    TYPES = ['NO_TYPE', # = 0 
             'TYPE_PAGE_URL', # = 1 the action is a URL to a page on the website being tracked.
             'TYPE_OUTLINK', # = 2: the action is a URL is of a link on the website being tracked. A visitor clicked it.
             'TYPE_DOWNLOAD', # = 3: the action is a URL of a file that was downloaded from the website being tracked.
             'TYPE_PAGE_TITLE', # = 4: the action is the page title of a page on the website being tracked.
             'TYPE_ECOMMERCE_ITEM_SKU', #= 5: the action is the SKU of an ecommerce item that is sold on the site.
             'TYPE_ECOMMERCE_ITEM_NAME', # = 6: the action is the name of an ecommerce item that is sold on the site.
             'TYPE_ECOMMERCE_ITEM_CATEGORY', # = 7: the action is the name of an ecommerce item category that is used on the site.
             'TYPE_SITE_SEARCH', # = 8: the action type is a site search action.
             'TYPE_EVENT_CATEGORY', # = 10: the action is an event category (see Tracking Events user guide)
             'TYPE_EVENT_ACTION', # = 11: the action is an event action
             'TYPE_EVENT_NAME', # = 12: the action is an event name
             'TYPE_CONTENT_NAME', # = 13: the action is a content name (see Content Tracking user guide and developer guide)
             'TYPE_CONTENT_PIECE', # = 14: the action is a content piece
             'TYPE_CONTENT_TARGET', # = 15: the action is a content target
             'TYPE_CONTENT_INTERACTION', # = 16: the action is a content interaction
    ]
    URL_PREFIXES = ['http://', # 0: 
                    'http://www.', # 1:
                    'https://', # 2:
                    'https://www.' #3:
                ]

    ALLOWED_SCHEMES = ['http','https']
    LOC_PATTERNS = [
        {
            'string': 'localhost',
            'label' : "LOCALTESTING"
        },
        {
            'string': 'dafneplus.northeurope.cloudapp.azure.com',
            'label' : "TESTING"
        },
        {
            'string': 'dafne-production.northeurope.cloudapp.azure.com',
            'label' : "TESTING"
        },
        {
            'string': 'your-ip-address',
            'label' : "TESTING"
        },
        {
            'string': '20.234.60.181|40.67.233.126',
            'label' : "TESTING"
        },
        {
            'string': 'dafneplus.eng.it',
            'label' : "VISIT"
        },
        {
            'string': 'bxss.me|dicrpdbjmemujemfyopp.zzz',
            'label' : "HACK"
        },
    ]

    PATH_PATTERNS = [
        {
            'string': r'/marketplace(?!-)/?|/nft-marketplace/?(?!-)|/sub-app-partner-nft-marketplace/?',
            'label' : "MARKETPLACE"
        },
        {
            'string': r'/wishlist/?',
            'label' : "WISHLIST"
        },
        {
            'string': r'/portfolio/?',
            'label' : "PORTFOLIO"
        },

        {
            'string': r'/moderation/?',
            'label' : "MODERATION"
        },
        {
            'string': r'/dao/?',
            'label' : "DAO"
        },
        {
            'string': r'/tool/?|/style-transfer|/virtual-avatar|/webpd|/nft-content-analysis|/pose-estimation|/3d-object-reconstruction|/sub-app-partner-vue|/sub-app-partner-content-creation-tools|/sub-app-vue',
            'label' : "TOOLS"
        },
        {
            'string': r'/team/?|/my-team/?',
            'label' : "TEAM"
        },
        {
            'string': r'/organization/?',
            'label' : "ORGANISATION"
        },
        {
            'string': r'/api/?|/marketplace-api/?|/nft-marketplace-api/?',
            'label' : "API"
        },
        {
            'string': r'/login/?',
            'label' : "LOGIN"
        },
        {
            'string': r'/register/?',
            'label' : "REGISTER"
        },
        {
            'string': r'/home/?|/$',
            'label' : "HOME"
        },
        {
            'string': r'/admin/?|/dozzle/?',
            'label' : "ADMIN"
        },
        {
            'string': r'/competitions/?',
            'label' : "COMPETITIONS"
        },
        {
            'string': r'/content/?',
            'label' : "CONTENT"
        },
        {
            'string': r'/reset-password/?|/restore-password/?',
            'label' : "PASSWORD"
        },
        {
            'string': r'/faq/?',
            'label' : "FAQ"
        },
        {
            'string': r'/profile/?',
            'label' : "PROFILE"
        },
        {
            'string': r'/privacy-policy?',
            'label' : "PRIVACYPOLICE"
        },        
    ]

    HACK_PATTERNS =[
        'sleep(',
        'XOR',
        'SELECT',
        'echo',
        'waitfor',
        'DBMS',
        'include src',
        '</ScRiPt>'
    ]
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
            # breakpoint()
    
    @classmethod
    def retrieve_entry(cls, id:int):
        if f'{id}' in cls.ActionsItems:
            return cls.ActionsItems[f'{id}']
        return None


    def __init__(self, idaction, name, type, url_prefix):
        self.idaction = idaction
        self.name = name
        self.type = self.TYPES[type]
        if url_prefix != None:
            self.url_prefix = self.URL_PREFIXES[url_prefix]
        else:
            self.url_prefix = None
    def __str__(self):
        return f'idaction {self.idaction}, name {self.name}, type {self.type}, url_prefix {self.url_prefix}'

class Action:

    def __init__(self, idlink_va:int, url:str, name:str, url_ref:str, name_ref:str,pageview_position:int,server_time:datetime.datetime):
        # breakpoint()
        self.idlink_va = idlink_va
        self.url = url
        self.name = name
        self.url_ref = url_ref
        self.name_ref = name_ref
        self.pageview_position = pageview_position
        self.server_time = server_time
        self.label = None
        self.sublabel = None

    def set_label(self, label:str, sublabel:str=None):
        self.label = label
        if sublabel != None:
            self.sublabel = sublabel

    def __str__(self):
        prtn = f'url {self.url}, name {self.name}, url_ref {self.url_ref}, name_ref {self.name_ref}, pageview_position {self.pageview_position}, ' \
        f'server_time {self.server_time}, label {self.label}, sublabel {self.sublabel}'
        return prtn
