# FBDP_Homework5
说明：
<br>
output文件夹中存放着簇中心和最终分类结果<br>
文件夹clusters-num中存放的是簇中心，clusetrInstance存放的是最终分类结果<br>
figure是可视化结果
文件命名中的“3、5”分别代表着3个簇和5个簇<br>
文件命名中的“5、10、20”分别代表着迭代次数
<br>
source_code文件夹中存放着源代码
KMeans.java是整个程序的入口<br>
RandomClusterGenerator.java是初始簇中心的生成器<br>
ClusterCenter.java的功能是不断迭代更新簇中心<br>
KMeansCluster.java的功能是根据最终的簇中心对整个数据集进行划分
