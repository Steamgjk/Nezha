import pandas as pd
from IPython import embed; 

LOGIN_PATH = "/home/steam1994"
FAST_REPLY = 6
SLOW_REPLY = 7
COMMIT_REPLY = 8

if __name__ == '__main__':
    num_replicas = 3
    num_proxies = 1
    num_clients = 2

    folder_name = "stats"
    stats_folder = "{login_path}/{folder_name}".format(
        login_path = LOGIN_PATH,
        folder_name = folder_name
    )
    client_df_list = []
    for i in range(num_clients):
        file_name = "Client-Stats-"+str(i+1)
        client_df = pd.read_csv(stats_folder+"/"+file_name)
        client_df_list.append(client_df)
    client_df = pd.concat(client_df_list)
    client_df['Latency'] = client_df['CommitTime']-client_df['SendTime']

    stats = ""
    stats += "Num:"+str(len(client_df))+"\n"
    stats += "50p:\t"+str(client_df['Latency'].quantile(.5))+"\n"
    stats += "75p:\t"+str(client_df['Latency'].quantile(.75))+"\n"
    stats += "90p:\t"+str(client_df['Latency'].quantile(.9))+"\n"
    fast_num = len(client_df[client_df['CommitType']== FAST_REPLY])
    stats += "Fast:\t"+str(fast_num/ len(client_df))+"\n"
    print(stats)

    # embed()
