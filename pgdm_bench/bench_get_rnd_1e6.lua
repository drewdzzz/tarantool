require('os').execute('rm *.xlog *.snap')
math.randomseed(42)

local iter_num = 1e6
local max_num = 4 * 1e6

data = {}
used = {}
used[0] = true

for i = 1, iter_num do
	num = 0
	while used[num] do
		num = math.random(1, max_num)
	end
	used[num] = true
	table.insert(data, num)
end

require('os').execute('rm *.xlog *.snap')
clock = require('clock')
box.cfg{}
s = box.schema.space.create('select_tester')
s:create_index('pk')
sk = s:create_index('sk', {type = 'pgdm'})

for i = 1, iter_num do
	s:replace{data[i]}
end

function bench_tree()
	for i = 1, max_num do
		s:get{i}
	end
end

function bench_pgdm()
	for i = 1, max_num do
		sk:get{i}
	end
end

-- Warmup
bench_tree()
bench_pgdm()

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

tree_rps = max_num / tree_avg_time 
pgdm_rps = max_num / pgdm_avg_time 

print('Tree RPS: ', tree_rps)
print('Pgdm RPS: ', pgdm_rps)
print('Gained ', (pgdm_rps - tree_rps) / tree_rps * 100, '%')

s:drop()
