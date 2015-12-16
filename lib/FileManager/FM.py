DEFAULT_ENCODING = 'utf-8'
REQUEST_DELAY = 2

encodings = [
    "ascii",
    "big5",
    "euc-jp",
    "euc-kr",
    "gb2312",
    "hz-gb-2312",
    "ibm855",
    "ibm866",
    "iso-2022-jp",
    "iso-2022-kr",
    "iso-8859-2",
    "iso-8859-5",
    "iso-8859-7",
    "iso-8859-8",
    "koi8-r",
    "maccyrillic",
    "shift_jis",
    "tis-620",
    "utf-8",
    "utf-16le",
    "utf-16be",
    "utf-32le",
    "utf-32be",
    "windows-1250",
    "windows-1251",
    "windows-1252",
    "windows-1253",
    "windows-1255"
]


# Aliases for FM actions
class Action(object):
    HOME = "FM.action.HomeFtp"
    REMOTE_FTP = "FM.action.RemoteFtp"
    LOCAL = 'FM.action.Local'
    SITE_LIST = 'FM.action.SiteList'

    REFRESH = 'FM.action.Refresh'

    OPEN_DIRECTORY = 'FM.action.Open'
    NAVIGATE = 'FM.action.Navigate'
    COPY_ENTRY = 'FM.action.CopyEntry'
    COPY_PATH = 'FM.action.CopyPath'

    UP = 'FM.action.Up'
    ROOT = 'FM.action.Root'

    VIEW = 'FM.action.View'
    EDIT = 'FM.action.Edit'
    CHMOD = 'FM.action.Chmod'
    COPY = 'FM.action.Copy'
    CREATE_COPY = 'FM.action.CreateCopy'
    MOVE = 'FM.action.Move'
    RENAME = 'FM.action.Rename'
    REMOVE = 'FM.action.Remove'

    NEW_FOLDER = 'FM.action.NewFolder'
    NEW_FILE = 'FM.action.NewFile'

    UPLOAD = 'FM.action.Upload'
    DOWNLOAD_BASIC = 'FM.action.DownloadBasic'
    DOWNLOAD_ARCHIVE = 'FM.action.DownloadArchive'
    DOWNLOAD_ZIP = 'FM.action.DownloadZip'
    DOWNLOAD_BZ2 = 'FM.action.DownloadBZ2'
    DOWNLOAD_GZIP = 'FM.action.DownloadGZip'
    DOWNLOAD_TAR = 'FM.action.DownloadTar'
    CREATE_ARCHIVE = 'FM.action.CreateArchive'
    EXTRACT_ARCHIVE = 'FM.action.ExtractArchive'

    SEARCH_FILES = 'FM.action.SearchFiles'
    SEARCH_TEXT = 'FM.action.SearchText'
    ANALYZE_SIZE = 'FM.action.AnalyzeSize'

    HTPASSWD = 'FM.action.Htpasswd'
    IP_BLOCK = 'FM.action.IPBlock'
    SHARE_ACCESS = 'FM.action.ShareAccess'
    SETTINGS = 'FM.action.Settings'

    HELP = 'FM.action.Help'
    LOGOUT = 'FM.action.Logout'


# Aliases for FM modules
class Module(object):
    HOME = "home"
    PUBLIC_FTP = "public_ftp"
