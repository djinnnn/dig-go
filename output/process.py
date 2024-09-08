import pandas as pd
import argparse

# 定义命令行参数解析器
def parse_args():
    parser = argparse.ArgumentParser(description="Extract unique IPs from a CSV file.")
    parser.add_argument('-i', '--input', required=True, help="Path to the input CSV file.")
    parser.add_argument('-o', '--output', required=True, help="Path to the output CSV file for unique IPs.")
    return parser.parse_args()

# 主函数
def main():
    # 解析命令行参数
    args = parse_args()
    
    # 读取输入 CSV 文件
    input_file = args.input
    output_file = args.output

    # 假设文件中有三列：domain, NS, IP
    df = pd.read_csv(input_file)

    # 检查输入数据
    print("Input DataFrame head:")
    print(df.head())

    # 去除空白行或无效的IP地址
    df_clean = df[df['IP'].notna()]

    # 获取唯一的 IP 地址
    unique_ips = df_clean['IP'].unique()

    # 将唯一 IP 地址转换为 DataFrame
    unique_ips_df = pd.DataFrame(unique_ips, columns=['Unique IPs'])

    # 输出到 CSV 文件
    unique_ips_df.to_csv(output_file, index=False)

    print(f"Unique IPs saved to {output_file}")

if __name__ == "__main__":
    main()
