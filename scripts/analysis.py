import pandas as pd
from IPython import embed; 
import argparse

LOGIN_PATH = "/home/steam1994"
FAST_REPLY = 6
SLOW_REPLY = 7
COMMIT_REPLY = 8

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--num_replicas',  type=int, default = 3,
                        help='Specify the number of replicas ')
    parser.add_argument('--num_proxies',  type=int, default = 2,
                        help='Specify the number of proxies ')
    parser.add_argument('--num_clients',  type=int, default = 10,
                        help='Specify the number of clients ')
    args = parser.parse_args()

    num_replicas = args.num_replicas
    num_proxies = args.num_proxies
    num_clients = args.num_clients

    print("replicas: ", num_replicas)
    print("proxies: ", num_proxies)
    print("clients: ", num_clients)


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
