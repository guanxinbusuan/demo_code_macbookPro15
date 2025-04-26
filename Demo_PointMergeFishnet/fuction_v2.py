## 更新 function.py 中的 方法 2025-04-26

# Version=2.0

import geopandas as gpd
import shapely
import pandas as pd
import seaborn as sns
from shapely.geometry import box
import numpy as np
from multiprocessing import Pool
import rtree
import os

import warnings
warnings.filterwarnings("ignore")


#生成单个网格的Shapely.geomerty.box类对象,create_fishnet的调用子方法
def process_grid(args):
    """
    生成单个网格的几何对象。

    :param args: 包含经度和纬度索引的元组
    :return: 生成的网格几何对象 (Polygon)
    """
    lon, lat, start_lon, start_lat, scale = args
    start_x = start_lon + lon * scale
    end_x = start_lon + (lon + 1) * scale
    start_y = start_lat + lat * scale
    end_y = start_lat + (lat + 1) * scale
    return box(start_x, start_y, end_x, end_y)



#筛选与输入gdf相交的网格(渔网)的id,create_fishnet的调用子方法
def filter_grids(args):
    """
    筛选与输入地理数据相交的网格。

    :param args: 包含网格索引、几何对象和候选几何对象索引的元组
    :return: 如果网格与输入地理数据相交，则返回网格索引；否则返回 None
    """
    idx, geom, candidates, shp = args
    for candidate in candidates:
        if geom.intersects(shp.geometry[candidate]):
            return idx
    return None



#创建渔网(栅格),更新为多线程方法，优化时间复杂度
def create_fishnet(shp, scale, cache_dir='cache'):
    """
    生成渔网（即矩形网格），并进一步优化以提高执行效率。

    :param shp: 输入需要被渔网化的地理数据 (geodataframe)
    :param scale: 网格大小（边长，单位为度）
    :param cache_dir: 缓存目录
    :return fishnet_gdf: 生成的渔网 (geodataframe, geometry=Polygon)
    """
    # 创建缓存目录
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f'fishnet_scale_{scale}.gpkg')

    # 检查缓存文件是否存在
    if os.path.exists(cache_path):
        print("Loading cached fishnet...")
        fishnet_gdf = gpd.read_file(cache_path)
        fishnet_gdf = fishnet_gdf[fishnet_gdf.intersects(shp.unary_union)]
        return fishnet_gdf

    # 计算地理边界
    bounds = shp.total_bounds
    start_lon = bounds[0]
    start_lat = bounds[1]
    end_lon = bounds[2]
    end_lat = bounds[3]

    # 计算网格数量
    grid_num_lon = int(np.ceil((end_lon - start_lon) / scale))
    grid_num_lat = int(np.ceil((end_lat - start_lat) / scale))

    # 并行生成网格
    args_list = [(lon, lat, start_lon, start_lat, scale) 
                 for lon in range(grid_num_lon) 
                 for lat in range(grid_num_lat)]

    with Pool() as pool:
        geometries = pool.map(process_grid, args_list)

    # 创建GeoDataFrame
    fishnet = {
        'geometry': geometries
    }
    fishnet_gdf = gpd.GeoDataFrame(fishnet, geometry='geometry', crs=shp.crs)

    # 预过滤：计算输入地理数据的边界缓冲区
    buffer_distance = scale  # 缓冲区距离（可调整）
    shp_buffer = shp.buffer(buffer_distance).unary_union

    # 利用缓冲区进行预筛选
    fishnet_gdf = fishnet_gdf[fishnet_gdf.intersects(shp_buffer)]

    # 使用空间索引加速筛选
    index = rtree.index.Index()
    for idx, geom in shp.geometry.items():  # 使用 items() 替代 iteritems()
        index.insert(idx, geom.bounds)

    # 批量查询候选几何对象
    candidate_indices = []
    for geom in fishnet_gdf.geometry:
        candidate_indices.append(list(index.intersection(geom.bounds)))

    # 并行筛选网格
    filter_args = [(idx, geom, candidate_indices[idx], shp) 
                   for idx, geom in enumerate(fishnet_gdf.geometry)]

    with Pool() as pool:
        results = pool.map(filter_grids, filter_args)

    # 筛选结果
    valid_indices = [result for result in results if result is not None]
    fishnet_gdf = fishnet_gdf.iloc[valid_indices].reset_index(drop=True)

    # 保存缓存结果
    fishnet_gdf.to_file(cache_path, driver='GPKG')

    return fishnet_gdf



#从包含点坐标的csv文件创建点gdf
def Create_Point_geopandas(csvfile_path):
    '''
    把含Lon经度和Lat维度的csv文件,转换geodataframe(geometry=Shapely.Point()类)

    :param csvfile_path: 含Lon经度和Lat维度的csv文件路径,string
    :return point_gdf: 点坐标的geodataframe(geometry=Shapely.Point()类)
    '''
    shp_df=pd.read_csv(csvfile_path)
    shp_df['Lon']=pd.to_numeric(shp_df['Lon'])
    shp_df['Lat']=pd.to_numeric(shp_df['Lat'])

    geometry_list=[]
    for lon,lat in zip(shp_df['Lon'],shp_df['Lat']):
        Point=shapely.Point(lon,lat)
        geometry_list.append(Point)
    
    point_gdf=gpd.GeoDataFrame(shp_df,geometry=geometry_list,crs="EPSG:4326")

    return point_gdf



#将点gdf混合进入面gdf(可以是行政边界shp，也可以是渔网gdf),返回融合后的新gdf
def Point_Merge_MultPolygon(point_gdf,data_gdf,point_op_id=None,add_new_attribute=False):
    '''
    将点point_gdf(新gdf)合并到data_gdf(旧gdf)中,返回其点在面中的数量count(Point_in_multpolygon)
    add_new_attribute控制逻辑,是否加入新属性(默认否);point_op_id如果加入新属性的点gdf的列名
    
    :param point_gdf: 输入的新点源geodataframe数据,geometry=Point
    :param data_gdf: 旧gdf数据 geometry=MultPolygon
    :param point_op_id: 点gdf的某个要加入旧面gdf的属性列名,string,default=None
    :param add_new_attribut: 是否加入新属性,还是只统计点在面数量,defalut=False(只统计点在面数量)
    :return result_gdf: 点Point和面Polygon合并后的新geodataframe(geometry=MultPolygon)
    '''
    #注意how参数和predicate参数的逻辑
    joined_gdf=gpd.sjoin(
        left_df=point_gdf,
        right_df=data_gdf,
        how='inner',
        predicate="intersects"
    )

    #做一个point属性表
    point_table = pd.DataFrame()
    point_table['count(Point_in_multpolygon)'] = joined_gdf.groupby('index_right').size()
    
    if add_new_attribute==True:
        point_table[point_op_id]=joined_gdf.groupby('index_right')[point_op_id].sum()
    else:
        pass
    
    # 合并新属性表和旧gdf,得到——> 合并后的兼有点源属性和旧gdf的新gdf
    # 左表(旧gdf)用原始索引，右表(point_table,新属性表)用 交汇索引(index_right)，用how参数定义合并方式）
    result_gdf = data_gdf.merge(point_table,
                                left_index=True,
                                right_on='index_right',
                                how='left')  #完全保留左表（原始gdf）
    
    
    # 保留原始面gdf数据的索引,之前漏了这一步，
    # 如果少了这一步处理，result_gdf.index会出现NaN
    result_gdf.set_index(data_gdf.index, inplace=True)
    
    return result_gdf
