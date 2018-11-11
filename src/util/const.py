holiday_text = 'holiday'
exchange_close_text = 'exchange close'
exchange_not_open_text = 'exchange not open yet'
exchange_lunch_break = 'exchange lunch break'

#conf
config_file = r'D:\predator-eagle\global_market\script\cfg\cfg.xml'
schema_file = r'D:\predator-eagle\global_market\script\cfg\cfg.xsd'
#config_file = r'D:\glen\src\cfg\cfg.xml'
#schema_file = r'D:\glen\src\cfg\cfg.xsd'

str_lxml = "lxml"
str_eikon_key = "eikon_key"
str_holiday_file = "holiday_file"
str_exchange = "exchange"
str_id = "id"

str_start_date = "SDate"
str_end_date = 'EDate'
int_zero = 0
int_one = 1
dot = '.'
white_space = ' '
splash = '/'
dash = '-'
colon = ':'
minus_one_float = -1.0
false = 'false'
true = 'true'

field_cf_date = 'CF_DATE'
field_cf_time = 'CF_TIME'
field_loc_dt = 'loc_DT'
date_fmt = '%Y-%m-%d'
time_fmt = '%H:%M:%S'
csv_file_ext = '.csv'

#eikon
fields_snapshot = ['CF_DATE', 'CF_TIME',
                   'OPEN_PRC', 'CF_HIGH', 'CF_LOW', 'CF_CLOSE',
                   'CF_VOLUME', 'CF_LAST', 'HST_CLOSE', 'ADJUST_CLS']
ek_portion_num = 200
# 30 sec
interval = 60

#tea
tea_file_format = 'qdddddddd'
tea_file_header = 'time OPEN_PRC CF_HIGH CF_LOW CF_CLOSE CF_VOLUME CF_LAST HST_CLOSE ADJUST_CLS'
tea_file_decimal = 2
tea_file_ts_fmt = '%Y-%m-%d %H:%M:%S'
tea_file_ext = '.tea'


#zmq
server_push_port = "5556"
server_pub_port = "5557"

#log
up_log_path = r'D:/predator-eagle/global_market/log/up/'
down_log_path = r'D:/predator-eagle/global_market/log/down/'
day_log_path = r'D:/predator-eagle/global_market/log/day/'
hist_log_path = r'D:/predator-eagle/global_market/log/hist/'
server_pub_info_log = '_server_pub_info.log'
server_pub_debug_log = '_server_pub_debug.log'
server_pub_err_log = '_server_pub_err.log'

client_info_log = '_client_Info.log'
client_debug_log = '_client_debug.log'
client_err_log = '_client_err.log'

main_info_log = '_main_Info.log'
log_yaml_file = r'util\logging.yaml'

data_pub_log = '_data_pub.log'
data_sub_log = '_data_sub.log'
day_file_log = '_day_file.log'
hist_file_log = '_hist_file.log'
hist_2_day_file_log = '_hist_2_day_file.log'
log_format = "%(asctime)s - %(name)s - %(filename)s - %(lineno)d - %(levelname)s - %(message)s"

#day
struct_fmt = 'iifffffffff'
day_file_ext = '.day'

#refdata
from_date = 'exDivDate'
to_date = 'endDate'
accum_adj_factor = 'accumAdjFactor'

#hist
#hist_start_date = '1992-01-02'