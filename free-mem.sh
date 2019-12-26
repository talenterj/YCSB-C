#释放页缓存
echo 1 > /proc/sys/vm/drop_caches
#腾出Dentries和Inode
echo 2 > /proc/sys/vm/drop_caches
#释放页面缓存，工具和索引节点
echo 3 > /proc/sys/vm/drop_caches
#刷新文件系统缓冲区
sync
