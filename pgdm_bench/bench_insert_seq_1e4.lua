require('os').execute('rm *.xlog *.snap')
clock = require('clock')
box.cfg{memtx_memory = 1024 * 1024 * 1024}
s = nil
sk = nil
function reset(type)
	if s ~= nil then
		s:drop()
	end
	s = box.schema.space.create('select_tester')
	s:create_index('pk')
	if type ~= nil then
		sk = s:create_index('sk', {type = type})
	end
end
local iter_num = 1e5

function bench()
	for i = 1, iter_num do
		s:replace{i}
	end
end

-- Warmup
for i = 1, 2 do
	reset('tree')
	bench()
	reset('pgdm')
	bench()
end

tree = {}
pgdm = {}

exp_num = 5

for i = 1, exp_num do
	reset()
	idle = clock.bench(bench)[1]
	reset('tree')
	time = clock.bench(bench)[1]
	table.insert(tree, time)
	print('Tree: ', time)
	reset('pgdm')
	time = clock.bench(bench)[1]
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
