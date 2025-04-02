# demo_code_macbookPro15

1. 学习与练习一些基本coding
2. 编写一些coding-demo

**log message:**

- 2025.4.2:
加入 Demo_PointMergeFishnet 目录：

#### 一个demo演示如何通过function.py写的API函数接口 实现 “边界渔网化+合并点源 与 渔网（面源）

>1. 利用数据文件：1.geojson的矢量边界 和 API 中的 create_fishnet 方法,实现 边界渔网化
>2. 利用数据文件：1.csv的dataframe,转换geodataframe 为 待合并的点源数据
>3. 利用 API 中的 Point_Merge_Fishnet 方法，实现 合并点源 与 渔网（面源） 得到全新的渔网