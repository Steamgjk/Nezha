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
    # client_df_list = []
    # for i in range(num_clients):
    #     file_name = "Client-Stats-"+str(i+1)
    #     client_df = pd.read_csv(stats_folder+"/"+file_name)
    #     client_df_list.append(client_df)
    # client_df = pd.concat(client_df_list)
    # client_df['Latency'] = client_df['CommitTime']-client_df['SendTime']

    # stats = ""
    # stats += "Num:"+str(len(client_df))+"\n"
    # stats += "50p:\t"+str(client_df['Latency'].quantile(.5))+"\n"
    # stats += "75p:\t"+str(client_df['Latency'].quantile(.75))+"\n"
    # stats += "90p:\t"+str(client_df['Latency'].quantile(.9))+"\n"
    # fast_num = len(client_df[client_df['CommitType']== FAST_REPLY])
    # stats += "Fast:\t"+str(fast_num/ len(client_df))+"\n"
    # print(stats)



    proxy_df_list = []
    for i in range(num_proxies):
        file_name = "Proxy-Stats-"+str(i+1)+".csv"
        proxy_df = pd.read_csv(stats_folder+"/"+file_name)
        proxy_df_list.append(proxy_df)
    proxy_df = pd.concat(proxy_df_list)


    proxy_df = proxy_df.sort_values(by=['ClientTime'])
    fast_df = proxy_df[proxy_df["SlowReplyTime"]==0].copy()
    slow_df = proxy_df[proxy_df["SlowReplyTime"]>0].copy()
    proxy_df['H1']=proxy_df['ProxyTime']-proxy_df["ClientTime"]
    proxy_df['H2']=proxy_df['RecvTime']-proxy_df["ProxyTime"]
    fast_df['F1']=fast_df['FastReplyTime']-fast_df["RecvTime"]
    slow_df['HF1']=slow_df['SlowReplyTime']-slow_df["RecvTime"]
    slow_df['HF3']=slow_df['SlowReplyTime']-slow_df["FastReplyTime"]
    fast_df['H3']=fast_df['ProxyRecvTime']-fast_df["FastReplyTime"]
    slow_df['H3']=slow_df['ProxyRecvTime']-slow_df["SlowReplyTime"]
    fast_df['bound'] = fast_df["Deadline"] - fast_df["ProxyTime"]
    slow_df['bound'] = slow_df["Deadline"] - slow_df["ProxyTime"]
    fast_df['total'] = fast_df["ProxyRecvTime"] - fast_df["ClientTime"]
    slow_df['total'] = slow_df["ProxyRecvTime"] - slow_df["ClientTime"]


    embed()
