package main

import (
    "bufio"
    "encoding/csv"
    "fmt"
    "os"
    "os/exec"
    "strings"
    "sync"
    "github.com/schollz/progressbar/v3"
	"regexp"
    "path/filepath"
    "flag"
)


func digNS(domain string, mu *sync.Mutex) map[string][]string {
	nsRegex := regexp.MustCompile(`\S+\s+IN\s+NS\s+(\S+)`)  // 匹配 NS 记录的正则表达式

    // 执行 dig 查询 NS 记录，并获取 ADDITIONAL 部分的 A 和 AAAA 记录
    cmd := exec.Command("dig", domain, "NS", "+additional")
    output, err := cmd.Output()
    if err != nil {
        fmt.Printf("[Error] Failed to execute dig command for domain %s: %v\n", domain, err)
        return nil
    }

    nsRecords := make(map[string][]string)
    scanner := bufio.NewScanner(strings.NewReader(string(output)))
    parsingAdditional := false

    for scanner.Scan() {
        line := scanner.Text()

        // 找到 ADDITIONAL SECTION
        if strings.Contains(line, ";; ADDITIONAL SECTION:") {
            parsingAdditional = true
            //continue
        }

        // 解析 ANSWER 部分中的 NS 记录
        if nsRegex.MatchString(line){
			matches := nsRegex.FindStringSubmatch(line)
			if len(matches) > 1 && !strings.Contains(line, "9.18.18-0ubuntu0.22.04.2-Ubuntu") {
				ns := matches[1]
				//println(ns,":",line)
				nsRecords[ns] = []string{} // 初始化 NS 记录
			}
		}

        // 解析 ADDITIONAL 部分中的 A 和 AAAA 记录
        if parsingAdditional {
			fmt.Printf("[DEBUG] Additional.\n")
            if strings.Contains(line, " IN A ") || strings.Contains(line, " IN AAAA ") {
                parts := strings.Fields(line)
                if len(parts) >= 5 {
                    ns := parts[0]
                    ip := parts[4]
                    if _, exists := nsRecords[ns]; exists {
                        nsRecords[ns] = append(nsRecords[ns], ip)
                        fmt.Printf("[DEBUG] Found IP %s for NS %s of domain %s\n", ip, ns, domain)
                    }
                }
            }
        }
    }

    return nsRecords
}


func digIP(ns string, mu *sync.Mutex) ([]string, []string) {
    var ipv4Records []string
    var ipv6Records []string

    // 查询 A 记录（IPv4）
    cmd := exec.Command("dig", ns, "A", "+short")
    output, err := cmd.Output()
    if err != nil {
        fmt.Printf("[Error] Failed to execute dig command for A record of NS %s: %v\n", ns, err)
    } else {
        scanner := bufio.NewScanner(strings.NewReader(string(output)))
        for scanner.Scan() {
            ipRecord := strings.TrimSpace(scanner.Text())
            //println(ns, ", ", ipRecord)
            if ipRecord != "" {
                ipv4Records = append(ipv4Records, ipRecord)
            }else {
				ipv4Records = append(ipv4Records, "")
			}
        }
    }

    // 查询 AAAA 记录（IPv6）
    cmd = exec.Command("dig", ns, "AAAA", "+short")
    output, err = cmd.Output()
    if err != nil {
        fmt.Printf("[Error] Failed to execute dig command for AAAA record of NS %s: %v\n", ns, err)
    } else {
        scanner := bufio.NewScanner(strings.NewReader(string(output)))
        for scanner.Scan() {
            ipRecord := strings.TrimSpace(scanner.Text())
            if ipRecord != "" {
                ipv6Records = append(ipv6Records, ipRecord)
            } else {
				ipv6Records = append(ipv6Records, "")
			}
        }
    }

    return ipv4Records, ipv6Records
}

func processDomain(domain string, mu *sync.Mutex, resultsIPv4 *[][]string, resultsIPv6 *[][]string, bar *progressbar.ProgressBar, sem chan struct{}) {
    defer func() { <-sem }() // 释放一个占用的通道

    nsRecords := digNS(domain, mu)
    var domainNSIPv4List [][]string
    var domainNSIPv6List [][]string

    if len(nsRecords) > 0 {
        for ns := range nsRecords {
			//println(ns)
            ipv4Records, ipv6Records := digIP(ns, mu)  // 查询 IPv4 和 IPv6 地址
            //println("124: ", ns,": ",ipv4Records)
            if len(ipv4Records) == 0 {
                domainNSIPv4List = append(domainNSIPv4List, []string{domain, ns, ""})  // 保存空 IP 的记录
            } else {
                for _, ip := range ipv4Records {
                    //println("126: ", ns)
                    domainNSIPv4List = append(domainNSIPv4List, []string{domain, ns, ip})  // 保存 IPv4 地址
                }
            }

            if len(ipv6Records) == 0 {
                domainNSIPv6List = append(domainNSIPv4List, []string{domain, ns, ""})  // 保存空 IP 的记录
            } else {
                for _, ip := range ipv6Records {
                    domainNSIPv6List = append(domainNSIPv6List, []string{domain, ns, ip}) // 保存 IPv6 地址
                }
            }
            
        }
    } else {
        domainNSIPv4List = append(domainNSIPv4List, []string{domain, "", ""}) // 没有 NS 记录
        domainNSIPv6List = append(domainNSIPv6List, []string{domain, "", ""})
    }

    mu.Lock()
    *resultsIPv4 = append(*resultsIPv4, domainNSIPv4List...)
    *resultsIPv6 = append(*resultsIPv6, domainNSIPv6List...)
    mu.Unlock()

    bar.Add(1)
}

func processFile(filename string, resultsIPv4 chan<- [][]string, resultsIPv6 chan<- [][]string, maxThreads int, cacheMutex *sync.Mutex, wgMain *sync.WaitGroup) {
    defer wgMain.Done()

    file, err := os.Open(filename)
    if err != nil {
        fmt.Println("Error opening file:", err)
        return
    }
    defer file.Close()

    reader := csv.NewReader(file)
    domains, err := reader.ReadAll()
    if err != nil {
        fmt.Println("Error reading CSV:", err)
        return
    }

    var wg sync.WaitGroup
    var mu sync.Mutex
    chunkResultsIPv4 := make([][]string, 0)
    chunkResultsIPv6 := make([][]string, 0)
    bar := progressbar.NewOptions(len(domains)-1,
        progressbar.OptionSetDescription("Processing domains in "+filename),
        progressbar.OptionSetPredictTime(true),
        progressbar.OptionShowCount(),
        progressbar.OptionFullWidth(),
        progressbar.OptionShowIts(),
    )

    sem := make(chan struct{}, maxThreads)

    for _, domain := range domains[1:] { // 跳过标题行
        wg.Add(1)
        sem <- struct{}{}
        go func(domain string) {
            defer wg.Done()
            processDomain(domain, &mu, &chunkResultsIPv4, &chunkResultsIPv6, bar, sem)
        }(domain[0])
    }

    wg.Wait()
    bar.Finish()

    fmt.Printf("[INFO] Sending %d IPv4 records and %d IPv6 records to results channels from file %s\n", len(chunkResultsIPv4), len(chunkResultsIPv6), filename)
    resultsIPv4 <- chunkResultsIPv4 // 确保所有 IPv4 结果发送到通道
    resultsIPv6 <- chunkResultsIPv6 // 确保所有 IPv6 结果发送到通道
}


func main() {
    chunkDir := flag.String("dir", "chunk-data", "The directory containing chunk files")
    flag.Parse()  // 解析命令行参数

    // 使用命令行传入的目录路径
    files, err := filepath.Glob(filepath.Join(*chunkDir, "chunk_*.csv"))
    resultsIPv4 := make(chan [][]string)
    resultsIPv6 := make(chan [][]string)
    finalResultsIPv4 := make([][]string, 0)
    finalResultsIPv6 := make([][]string, 0)
    var cacheMutex sync.Mutex  // cacheMutex 虽然不再需要 cache，但仍需要锁来处理并发
    var wgMain sync.WaitGroup
    var finalResultsMutex sync.Mutex
    var resultsWaitGroup sync.WaitGroup

    resultsWaitGroup.Add(2)  // 等待两个通道的处理完成

    // 处理 IPv4 结果
    go func() {
        defer resultsWaitGroup.Done()
        for res := range resultsIPv4 {
            finalResultsMutex.Lock()
            fmt.Printf("[INFO] Received %d IPv4 records, processing...\n", len(res))
            finalResultsIPv4 = append(finalResultsIPv4, res...)
            fmt.Printf("[INFO] Total IPv4 records in finalResults: %d\n", len(finalResultsIPv4))
            finalResultsMutex.Unlock()
        }
    }()

    // 处理 IPv6 结果
    go func() {
        defer resultsWaitGroup.Done()
        for res := range resultsIPv6 {
            finalResultsMutex.Lock()
            fmt.Printf("[INFO] Received %d IPv6 records, processing...\n", len(res))
            finalResultsIPv6 = append(finalResultsIPv6, res...)
            fmt.Printf("[INFO] Total IPv6 records in finalResults: %d\n", len(finalResultsIPv6))
            finalResultsMutex.Unlock()
        }
    }()

    maxThreads := 50

    for _, file := range files {
        wgMain.Add(1)
        go processFile(file, resultsIPv4, resultsIPv6, maxThreads, &cacheMutex, &wgMain)  // 移除 cache 参数
    }

    wgMain.Wait()  // 等待所有文件处理完成
    close(resultsIPv4) // 确保所有 IPv4 数据传输完成后再关闭通道
    close(resultsIPv6) // 确保所有 IPv6 数据传输完成后再关闭通道

    resultsWaitGroup.Wait()  // 等待所有数据接收完毕

    // 写入 IPv4 结果
    fmt.Printf("[INFO] Writing %d IPv4 results to file\n", len(finalResultsIPv4))
    outFileIPv4, err := os.Create("output/auth-ns-ipv4.csv")
    if err != nil {
        fmt.Println("Error creating IPv4 output file:", err)
        return
    }
    defer outFileIPv4.Close()

    writerIPv4 := csv.NewWriter(outFileIPv4)
    defer writerIPv4.Flush()

    writerIPv4.Write([]string{"domain", "NS", "IP"})
    writerIPv4.WriteAll(finalResultsIPv4)

    // 写入 IPv6 结果
    fmt.Printf("[INFO] Writing %d IPv6 results to file\n", len(finalResultsIPv6))
    outFileIPv6, err := os.Create("output/auth-ns-ipv6.csv")
    if err != nil {
        fmt.Println("Error creating IPv6 output file:", err)
        return
    }
    defer outFileIPv6.Close()

    writerIPv6 := csv.NewWriter(outFileIPv6)
    defer writerIPv6.Flush()

    writerIPv6.Write([]string{"domain", "NS", "IP"})
    writerIPv6.WriteAll(finalResultsIPv6)

    fmt.Println("[INFO] Finished writing results to output/auth-tld-ns-ipv4.csv and output/auth-tld-ns-ipv6.csv")
}
