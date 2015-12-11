牛客网机器学习第一课
	Mapreduce 
	 计算用户的相似度


 基于用户安装app的数据计算用户的相似度

input:
	userA：app1，app2，app3
	userB：app2，app3，app4

output:
	userA和userB的相关性

计算公式：Jaccard距离   J(A,B)=|A-B|/|A+B|			
							- : 交集
 							+ : 并集 )
本次作业数据：电影 id  ： 用户id
		(input)						mid   uid1;uid2;.....	
			
			
			四个jod(四个 mapper reducer) 														input         						  output
						1:计算每个user 看的电影的数量   										input								   count
						2:计算每两个用户的交集                                 						input           					 intersection
						3.key: 和第二步的key一样 		value: count1:count2		count									job3
						4:最终结果，求出每两个用户的相似度								intersection/job3			job4