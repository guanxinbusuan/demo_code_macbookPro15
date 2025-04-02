import geopandas as gpd
from shapely.geometry import box
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def create_fishnet(shp, scale):
    """

    计算 -> 将某个矢量边界和其内部(geodataframe)进行 渔网化(注意区分和栅格化的差异);
    生成 -> 渔网 (geometry=面几何Polygon的面源geodataframe).

    :param shp: 输入需要被渔网化的地理数据 (geodataframe)
    :param scale: 网格大小（边长，单位为度）
    :return fishnet_gdf :生成的渔网 (geodataframe)

    """
    bounds = shp.total_bounds
    start_lon = bounds[0]
    start_lat = bounds[1]

    grid_num_lon = int(np.ceil((bounds[2] - bounds[0]) / scale))
    grid_num_lat = int(np.ceil((bounds[3] - bounds[1]) / scale))

    fishnet = {
        'number': [],
        'start_lon': [],
        'end_lon': [],
        'start_lat': [],
        'end_lat': [],
        'geometry': []
    }

    for lon in range(grid_num_lon):
        for lat in range(grid_num_lat):
            fishnet['number'].append((lon+1) * (lat+1))
            fishnet['start_lon'].append(start_lon + lon * scale)
            fishnet['end_lon'].append(start_lon + (lon+1) * scale)
            fishnet['start_lat'].append(start_lat + lat * scale)
            fishnet['end_lat'].append(start_lat + (lat+1) * scale)
            fishnet['geometry'].append(box(
                start_lon + lon * scale,
                start_lat + lat * scale,
                start_lon + (lon+1) * scale,
                start_lat + (lat+1) * scale
            ))

    fishnet_gdf = gpd.GeoDataFrame(fishnet, geometry='geometry', crs=shp.crs)
    fishnet_gdf = fishnet_gdf[fishnet_gdf.intersects(shp.union_all())]
    return fishnet_gdf




def Point_Merge_Fishnet(point_gdf, fishnet_gdf,new_attribute_name):
    """

    合并 -> 新输入的(geometry=点几何Point的点源geodataframe)地理数据 与 之前得到的旧渔网(geodataframe,面源类型地理数据),
    生成 -> 全新属性和保留了全部旧渔网属性的 新渔网(geodataframe)

    :param point_gdf: 输入的新点源地理数据 (geodataframe)
    :param fishnet_gdf: 旧渔网地理数据 (geodataframe)
    :param new_attribute_name (string): 将新点源地理数据中的某一种属性加入旧渔网,作为以后再运行某些算法的保留数据列
    :return result_fishnet: 点Point和面Polygon合并后的 新渔网(geodataframe)

    """

    # 创建过渡表——空间连接表（geodataframe）
    # 空间连接——>找出点源数据(geodataframe) 落在 旧渔网（geodataframe）内的点
    joined = gpd.sjoin(point_gdf, fishnet_gdf, how='inner', predicate='within')

    # 聚合空间连接过渡表，得到新属性表point_table
    point_table = pd.DataFrame()
    # 按旧渔网的网格索引index_right（也是点、面交汇索引），聚合得到：点源数据将落在旧渔网网格中的点的数量
    point_table['count'] = joined.groupby('index_right').size()
    # 按旧渔网的网格索引index_right（也是点、面交汇索引），聚合得到：点源数据中需要再运算的属性值
    point_table[new_attribute_name]= joined.groupby('index_right')[new_attribute_name].sum()


    # 合并新属性表和原始渔网,得到——> 合并后的兼有点源属性和旧渔网网格的新渔网geodataframe
    # 左表(旧渔网)用原始索引，右表(point_table,新属性表)用 交汇索引(index_right)，用how参数定义合并方式）
    result_fishnet = fishnet_gdf.merge(point_table,
                                left_index=True,
                                right_on='index_right',
                                how='left')  #完全保留左表（原始渔网）


    return result_fishnet

if __name__=="__main__":
    #步骤一：利用数据文件1.geojson的矢量边界 和 API 中的 create_fishnet 方法,实现 边界渔网化
    profile_data = gpd.read_file('Demo_PointMergeFishnet/1.json')
    fishnet_old=create_fishnet(profile_data,0.01)

    #步骤二：利用数据文件1.csv的dataframe,转换geodataframe 为 待合并的点源数据
    from shapely import Point
    #注意geomerty都是经度在前，纬度在后！！！！！
    point_data=pd.read_csv('Demo_PointMergeFishnet/1.csv')
    point_data['geometry']=[Point(x,y) for x,y in zip(point_data['lat'],point_data['lon'])]
    # 转换为 GeoDataFrame 并设置初始 CRS
    point_data = gpd.GeoDataFrame(point_data,crs="EPSG:4326")


    #步骤三： 利用 API 中的 Point_Merge_Fishnet 方法，实现 合并点源 与 渔网（面源） 得到 全新的渔网
    new_fishnet=Point_Merge_Fishnet(point_data,fishnet_old,'value')

    #绘图，示意一下
    fig,ax=plt.subplots()
    new_fishnet.plot(ax=ax, color='white', edgecolor='black')
    new_fishnet.plot(column='value', cmap='Blues',legend=True,ax=ax)
    plt.show()
