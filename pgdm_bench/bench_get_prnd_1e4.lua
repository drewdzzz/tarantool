require('os').execute('rm *.xlog *.snap')
clock = require('clock')
box.cfg{}
s = box.schema.space.create('select_tester')
s:create_index('pk')
sk = s:create_index('sk', {type = 'pgdm'})
local iter_num = 1e4

data = {}
for i = 1, iter_num do
	local pos = math.random(1, #data + 1)
	table.insert(data, pos, i)
end

for i = 1, iter_num do
	s:replace{data[i]}
end

function bench_tree()
	for i = 1, iter_num do
		s:get{i}
	end
end

function bench_pgdm()
	for i = 1, iter_num do
		sk:get{i}
	end
end

-- Warmup
for i = 1, 3 do
	bench_tree()
	bench_pgdm()
end

tree = {}
pgdm = {}

exp_num = 5

for i = 1, exp_num do
	time = clock.bench(bench_tree)[1]
	table.insert(tree, time)
	print('Tree: ', time)
	time = clock.bench(bench_pgdm)[1]
	table.insert(pgdm, time)
	print('Pgdm: ', time)
end

tree_avg_time = 0
pgdm_avg_time = 0
for i = 1, exp_num do
	tree_avg_time = tree_avg_time + tree[i]
	pgdm_avg_time = pgdm_avg_time + pgdm[i]
end
tree_avg_time = tree_avg_time / 5
pgdm_avg_time = pgdm_avg_time / 5

tree_rps = iter_num / tree_avg_time 
pgdm_rps = iter_num / pgdm_avg_time 

print('Tree RPS: ', tree_rps)
print('Pgdm RPS: ', pgdm_rps)
print('Gained ', (pgdm_rps - tree_rps) / tree_rps * 100, '%')

s:drop()
