### URL TopN
This problem is similar to this SQL ` SELECT url,c FROM (SELECT url,COUNT(url) as c FROM t GROUP BY url) ORDERBY c DESC LIMIT 100`, so I used a similar plan.
```
                            +-------------+
                            | Final TopN |
                            +------+------+

                    ^                               ^ 
                    |  partial TopN                 |  partial TopN
                    +                               +
              +---+------------+            +----+-----------+
              | bucket worker |   ......    | bucket worker |
              +--------------+-+            +-+--------------+

                                   ^
                                   |  hash(row)
                                   +
                            +-------------+
                            | Scan Thread |
                            +------+------+

```

#### Performance

##### Harware
- CPUT: Intel(R) Xeon(R) CPU E5-2682 v4 @ 2.50GHz   X 2
- Memory: 512GB
- Disk: 8TB SATA

##### Test
- Generator test Data
```bash
go run test/generator.go -out /disk4/test/urls.txt -count 37000000
```
It will generate a aoublt 113g data file

- Run TopN
```go
go build
./topN

....

cost: 3m6.207592496s
```