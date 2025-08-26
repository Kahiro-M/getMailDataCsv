#日付を取得
import datetime as dt
#フォルダの存在確認
import os
import sys

#------------------------------------------------#
# 現在時刻のフォルダ作成
#------------------------------------------------#
def mkdir_datetime(folder_start='',folder_end=''):
    dt_now = dt.datetime.now()
    timestamp_str = dt_now.strftime('%Y%m%d_%H%M_%S')
    # 現在のフォルダパス(プログラムが実行されているフォルダパス)
    currentDir = os.path.dirname(os.path.abspath(sys.argv[0]))
    # 作成フォルダ名
    resultDirName = folder_start + timestamp_str + folder_end
    # フォルダのフルパス
    resultDirFullPath = currentDir + '\\' + resultDirName

    if(not (os.path.exists(resultDirFullPath))):
        os.mkdir(resultDirFullPath)
    
    return resultDirFullPath

#------------------------------------------------#
# 今日の日付を取得
#------------------------------------------------#
def get_today_date(split_str='/'):
    dt_now = dt.datetime.now()
    timestamp_str = dt_now.strftime('%Y'+split_str+'%m'+split_str+'%d')
    return timestamp_str

#------------------------------------------------#
# 時刻を取得
#------------------------------------------------#
def get_now_time(split_str=':'):
    dt_now = dt.datetime.now()
    timestamp_str = dt_now.strftime('%H'+split_str+'%M'+split_str+'%S')
    return timestamp_str

if __name__ == '__main__':
    mkdir_datetime()
