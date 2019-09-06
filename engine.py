f = open("./files/metadata.txt", "r")
x = f.readlines()
f.close()

tables_metadata = {}
it = 0
while it < len(x):
	y = []
	if x[it][:2] == "<b":
		it += 1
	while x[it][:2] != "<e":
		y.append(x[it].strip())
		it += 1
	it += 1
	tables_metadata[y[0]] = y[1:]

tables_data = {}
for t in tables_metadata.keys():
	f = open("./files/{}.csv".format(t), "r")
	x = f.readlines()
	f.close()
	y = []
	for row in x:
		row = row.strip().split(",")
		for i in range(len(row)):
			row[i] = int(row[i].strip())
		y.append(row)
	tables_data[t] = y

print("Enter your query:")
query = input().strip()

try:
	select_idx = query.index("select")
except ValueError:
	print("ERROR: 'select' not found")
	exit()

try:
	from_idx = query.index("from")
except ValueError:
	print("ERROR: 'from' not found")
	exit()

try:
	where_idx = query.index("where")
except ValueError:
	where_idx = -1

_select = query[(select_idx+7):from_idx].strip()
_from = None
_where = None
if where_idx == -1:
	_from = query[(from_idx+5):].strip()
else:
	_from = query[(from_idx+5):where_idx].strip()
	_where = query[(where_idx+6):].strip()

tables_in_from = _from.split(",")
if len(tables_in_from) == 0:
	print("ERROR: no tables provided")
	exit()
for i in range(len(tables_in_from)):
	tables_in_from[i] = tables_in_from[i].strip()
	if tables_in_from[i] not in tables_metadata.keys():
		print('ERROR: invalid table name(s)')
		exit()

query_table = tables_data[tables_in_from[0]]
query_table_cols = tables_metadata[tables_in_from[0]]
query_table_cols = list(map(lambda x: tables_in_from[0] + '.' + x, query_table_cols))

it = 1
while it < len(tables_in_from):
	temp = []
	cols = tables_metadata[tables_in_from[it]]
	cols = list(map(lambda x: tables_in_from[it] + '.' + x, cols))
	for row1 in query_table:
		for row2 in tables_data[tables_in_from[it]]:
			temp.append(row1 + row2)
	query_table = temp
	query_table_cols += cols
	it += 1

idx_in_joined = {}
idx_in_joined[tables_in_from[0]] = 0
it = 1
while it < len(tables_in_from):
	if tables_in_from[it] not in idx_in_joined.keys():	
		idx_in_joined[tables_in_from[it]] = len(tables_metadata[tables_in_from[it - 1]])
	it += 1

aggregate_funcs = ['sum', 'min', 'max', 'avg']

distinct = (_select[:8] == "distinct")
if distinct:
	_select = _select[9:]

if _select == '*':
	for i in range(len(query_table_cols)):
		if i == len(query_table_cols) - 1:
			print(query_table_cols[i])
		else:
			print(query_table_cols[i], end=', ')
	if distinct:
		t = set()
		for r in query_table:
			t.add(tuple(r))
		query_table = t
	for row in query_table:
		for i in range(len(row)):
			if i == len(row) - 1:
				print(row[i])
			else:
				print(row[i], end=', ')
	exit()

elif (_select[:3] in aggregate_funcs):
	try:
		x1 = _select.index('(')
	except:
		print("ERROR: invalid syntax")
		exit()
	try:
		x2 = _select.index(')')
	except:
		print("ERROR: invalid syntax")
		exit()
	col = _select[(1+x1):x2]
	try:
		x = col.index(".")
		table_name = col[:x]
		col = col[(1+x):]
	except:
		for t in tables_metadata.keys():
			if col in tables_metadata[t]:
				table_name = t
				break
	col_idx = tables_metadata[table_name].index(col)
	col_data = []
	for r in tables_data[table_name]:
		col_data.append(r[col_idx])
	func = _select[:3]
	print(func + " of " + table_name + "." + col)
	if len(col_data) == 0:
		print("WARNING: there are no elements satisfying the query")
		exit()
	if func == "sum":
		print(sum(col_data))
	elif func == "avg":
		print(sum(col_data) / len(col_data))
	elif func == "max":
		print(max(col_data))
	elif func == "min":
		print(min(col_data))
	else:
		print("ERROR: no such aggregate function")
		exit()

else:
	cols = _select.split(',')
	for i in range(len(cols)):
		cols[i] = cols[i].strip()
		if '.' not in cols[i]:
			for t in tables_metadata.keys():
				if cols[i] in tables_metadata[t]:
					cols[i] = t + '.' + cols[i]
					break
		if '.' not in cols[i]:
			print("ERROR: no column called {} found in databse".format(cols[i]))
			exit()
	idx = []
	for i in range(len(cols)):
		if i == len(cols) - 1:
			print(cols[i])
		else:
			print(cols[i], end=', ')
		table_name = cols[i][:(cols[i].index("."))]
		col_name = cols[i][(1 + cols[i].index(".")):]
		j = idx_in_joined[table_name] + tables_metadata[table_name].index(col_name)
		idx.append(j)
	ans = []
	for r in query_table:
		y = []
		for i in idx:
			y.append(r[i])
		ans.append(y)
	if distinct:
		t = set()
		for r in ans:
			t.add(tuple(r))
		query_table = list(t)
	for r in query_table:
		for i in range(len(r)):
			if i == len(r) - 1:
				print(r[i])
			else:
				print(r[i], end=', ')
