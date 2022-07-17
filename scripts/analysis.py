import pandas as pd
from IPython import embed; 
import argparse
import datetime

LOGIN_PATH = "/home/steam1994"
FAST_REPLY = 6
SLOW_REPLY = 7
COMMIT_REPLY = 8


def throughput_apply_func(group):
    if len(group):
        return pd.Series({
            'AvgThroughput':len(group),
        })

def ThroughputAnalysis(merge_df):
    merge_df.loc[:, "time"] = merge_df['CommitTime'].apply(
                lambda us_ts: datetime.datetime.fromtimestamp(us_ts * 1e-6))
    bin_interval_s = 1
    grouped = merge_df.groupby(
        pd.Grouper(key='time', freq='{}s'.format(bin_interval_s)))
    grouped_apply_orders = grouped.apply(throughput_apply_func)
    grouped_apply_orders = grouped_apply_orders.dropna()
    grouped_apply_orders = grouped_apply_orders[5:-5]
    # print(grouped_apply_orders['AvgThroughput'])
    throughput = (grouped_apply_orders['AvgThroughput']/bin_interval_s).mean()
    return throughput


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

    throughput_stats = ThroughputAnalysis(client_df)
    print("Throughput ", throughput_stats)



    proxy_df_list = []
    for i in range(num_proxies):
        file_name = "Proxy-Stats-"+str(i+1)+".csv"
        proxy_df = pd.read_csv(stats_folder+"/"+file_name)
        proxy_df_list.append(proxy_df)
        print("Proxy ", len(proxy_df))
    proxy_df = pd.concat(proxy_df_list)

    proxy_df = proxy_df.sort_values(by=['ClientTime'])
    proxy_df["E2E"] = proxy_df["ProxyRecvTime"]-proxy_df["ProxyTime"]
    proxy_df["Bound"] = proxy_df["Deadline"]-proxy_df["ProxyTime"]
    fast_num = len(proxy_df[proxy_df["CommitType"]==6])
    print("fast commit ratio ", fast_num/len(proxy_df))
    print("Bound ", proxy_df["Bound"].quantile(.5))
    print("Proxy-E2E  50p ", proxy_df["E2E"].quantile(.5), 
            "\t75p:", proxy_df["E2E"].quantile(.75),
            "\t90p:", proxy_df["E2E"].quantile(.9),
            "\t95p:", proxy_df["E2E"].quantile(.95))


    # fast_df = proxy_df[proxy_df["SlowReplyTime"]==0].copy()
    # slow_df = proxy_df[proxy_df["SlowReplyTime"]>0].copy()
    # proxy_df['H1']=proxy_df['ProxyTime']-proxy_df["ClientTime"]
    # proxy_df['H2']=proxy_df['RecvTime']-proxy_df["ProxyTime"]
    # fast_df['F1']=fast_df['FastReplyTime']-fast_df["RecvTime"]
    # slow_df['HF1']=slow_df['SlowReplyTime']-slow_df["RecvTime"]
    # slow_df['HF3']=slow_df['SlowReplyTime']-slow_df["FastReplyTime"]
    # fast_df['H3']=fast_df['ProxyRecvTime']-fast_df["FastReplyTime"]
    # slow_df['H3']=slow_df['ProxyRecvTime']-slow_df["SlowReplyTime"]
    # fast_df['total'] = fast_df["ProxyRecvTime"] - fast_df["ClientTime"]
    # slow_df['total'] = slow_df["ProxyRecvTime"] - slow_df["ClientTime"]


    embed()
