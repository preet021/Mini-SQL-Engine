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

rep_col = False
t1 = None
t2 = None
column1 = None
column2 = None

if where_idx != -1:
	try:
		and_idx = _where.index("AND")
	except:
		and_idx = -1
	try:
		or_idx = _where.index("OR")
	except:
		or_idx = -1
	cond_join = False
	if and_idx != -1:
		c1 = _where[:and_idx].strip()
		c2 = _where[(4+and_idx):].strip()
	elif or_idx != -1:
		c1 = _where[:or_idx].strip()
		c2 = _where[(3+or_idx):].strip()
	elif and_idx == -1 and or_idx == -1:
		c1 = _where.strip()
		it = 0
		while (it < len(c1) and (ord(c1[it]) not in [60, 61, 62])):
			it += 1
		col1 = c1[:it].strip()
		rel_op1 = ""
		while (it < len(c1) and (ord(c1[it]) in [60, 61, 62])):
			rel_op1 += c1[it]
			it += 1
		try:
			v1 = int(c1[it:].strip())
		except:
			cond_join = True
		if '.' not in col1:
			for t in tables_in_from:
				if col1 in tables_metadata[t]:
					idx1 = idx_in_joined[t] + tables_metadata[t].index(col1)
		else:
			it = col1.index('.')
			idx1 = idx_in_joined[col1[:it]] + tables_metadata[col1[:it]].index(col1[(1+it):])

		if not cond_join:
			temp = []
			for r in query_table:
				if rel_op1 == "=":
					if r[idx1] == v1:
						temp.append(r)
				elif rel_op1 == "<":
					if r[idx1] < v1:
						temp.append(r)
				elif rel_op1 == ">":
					if r[idx1] > v1:
						temp.append(r)
				elif rel_op1 == ">=":
					if r[idx1] >= v1:
						temp.append(r)
				elif rel_op1 == "<=":
					if r[idx1] <= v1:
						temp.append(r)
			query_table = temp
		else:
			it = 0
			while (it < len(_where)) and (ord(c1[it]) not in [60, 61, 62]):
				it += 1
			col1 = _where[:it].strip()
			rel_op = ""
			while (it < len(c1) and (ord(c1[it]) in [60, 61, 62])):
				rel_op += _where[it]
				it += 1
			col2 = _where[it:].strip()
			if '.' not in col1:
				for t in tables_in_from:
					if col1 in tables_metadata[t]:
						t1 = t + col1
						idx1 = idx_in_joined[t] + tables_metadata[t].index(col1)
			else:
				t1 = col1
				it = col1.index('.')
				idx1 = idx_in_joined[col1[:it]] + tables_metadata[col1[:it]].index(col1[(1+it):])
		
			if '.' not in col2:
				for t in tables_in_from:
					if col2 in tables_metadata[t]:
						t2 = t + col2
						idx2 = idx_in_joined[t] + tables_metadata[t].index(col2)
			else:
				t2 = col2
				it = col2.index('.')
				idx2 = idx_in_joined[col2[:it]] + tables_metadata[col2[:it]].index(col2[(1+it):])
			temp = []
			for r in query_table:
				if rel_op == '<':
					if r[idx1] < r[idx2]:
						temp.append(r)
				if rel_op == '<=':
					if r[idx1] <= r[idx2]:
						temp.append(r)
				if rel_op == '>=':
					if r[idx1] >= r[idx2]:
						temp.append(r)
				if rel_op == '>':
					if r[idx1] > r[idx2]:
						temp.append(r)
				if rel_op == '=':
					if r[idx1] == r[idx2]:
						rep_col = True
						temp.append(r)
			query_table = temp

	if and_idx != -1 or or_idx != -1:
		it = 0
		while (it < len(c1)) and (ord(c1[it]) not in [60, 61, 62]):
			it += 1
		col1 = c1[:it].strip()
		rel_op1 = ""
		while (it < len(c1) and (ord(c1[it]) in [60, 61, 62])):
			rel_op1 += c1[it]
			it += 1
		v1 = int(c1[it:].strip())
		
		it = 0
		while (it < len(c2) and (ord(c2[it]) not in [60, 61, 62])):
			it += 1
		col2 = c2[:it].strip()
		rel_op2 = ""
		while (it < len(c2) and (ord(c2[it]) in [60, 61, 62])):
			rel_op2 += c2[it]
			it += 1
		v2 = int(c2[it:].strip())
		
		if '.' not in col1:
			for t in tables_in_from:
				if col1 in tables_metadata[t]:
					idx1 = idx_in_joined[t] + tables_metadata[t].index(col1)
		else:
			it = col1.index('.')
			idx1 = idx_in_joined[col1[:it]] + tables_metadata[col1[:it]].index(col1[(1+it):])
		
		if '.' not in col2:
			for t in tables_in_from:
				if col2 in tables_metadata[t]:
					idx2 = idx_in_joined[t] + tables_metadata[t].index(col2)
		else:
			it = col2.index('.')
			idx2 = idx_in_joined[col2[:it]] + tables_metadata[col2[:it]].index(col2[(1+it):])
	if and_idx != -1:
		temp = []
		for r in query_table:
			if rel_op1 == "=":
				if r[idx1] == v1:
					temp.append(r)
			elif rel_op1 == "<":
				if r[idx1] < v1:
					temp.append(r)
			elif rel_op1 == ">":
				if r[idx1] > v1:
					temp.append(r)
			elif rel_op1 == ">=":
				if r[idx1] >= v1:
					temp.append(r)
			elif rel_op1 == "<=":
				if r[idx1] <= v1:
					temp.append(r)
		query_table = []
		for r in temp:
			if rel_op2 == "=":
				if r[idx2] == v2:
					query_table.append(r)
			elif rel_op2 == "<":
				if r[idx2] < v2:
					query_table.append(r)
			elif rel_op2 == ">":
				if r[idx2] > v2:
					query_table.append(r)
			elif rel_op2 == ">=":
				if r[idx2] >= v2:
					query_table.append(r)
			elif rel_op2 == "<=":
				if r[idx2] <= v2:
					query_table.append(r)
	if or_idx != -1:
		temp = []
		for r in query_table:
			if rel_op1 == "=":
				if r[idx1] == v1:
					temp.append(r)
			elif rel_op1 == "<":
				if r[idx1] < v1:
					temp.append(r)
			elif rel_op1 == ">":
				if r[idx1] > v1:
					temp.append(r)
			elif rel_op1 == ">=":
				if r[idx1] >= v1:
					temp.append(r)
			elif rel_op1 == "<=":
				if r[idx1] <= v1:
					temp.append(r)
		for r in query_table:
			if rel_op2 == "=":
				if r[idx2] == v2:
					temp.append(r)
			elif rel_op2 == "<":
				if r[idx2] < v2:
					temp.append(r)
			elif rel_op2 == ">":
				if r[idx2] > v2:
					temp.append(r)
			elif rel_op2 == ">=":
				if r[idx2] >= v2:
					temp.append(r)
			elif rel_op2 == "<=":
				if r[idx2] <= v2:
					temp.append(r)
		temp1 = set()
		for r in temp:
			temp1.add(tuple(r))
		query_table = []
		for r in temp1:
			query_table.append(list(r))

aggregate_funcs = ['sum', 'min', 'max', 'avg']

distinct = (_select[:8] == "distinct")
if distinct:
	_select = _select[9:]

rep_col = []
if t1:
	rep_col.append(t1)
	rep_col.append(t2)
rep_col_cnt = 0

if _select == '*':
	for i in range(len(query_table_cols)):
		if query_table_cols[i] in rep_col:
			rep_col_cnt += 1
	idx = -1
	if rep_col_cnt == 2:
		idx = query_table_cols.index(t1)
	for i in range(len(query_table_cols)):
		if idx == i:
			continue
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
			if idx == i:
				continue
			if i == len(row) - 1:
				print(row[i])
			else:
				print(row[i], end=', ')

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
		for t in tables_in_from:
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
			for t in tables_in_from:
				if cols[i] in tables_metadata[t]:
					cols[i] = t + '.' + cols[i]
					if cols[i] in rep_col:
						rep_col_cnt += 1
					break
		if '.' not in cols[i]:
			print("ERROR: no column called {} found in database".format(cols[i]))
			exit()
	idx1 = -1
	idx = []
	if rep_col_cnt == 2:
		idx1 = cols.index(t1)
	for i in range(len(cols)):
		if idx1 == i:
			continue
		table_name = cols[i][:(cols[i].index("."))]
		col_name = cols[i][(1 + cols[i].index(".")):]
		j = idx_in_joined[table_name] + tables_metadata[table_name].index(col_name)
		idx.append(j)
		if i == len(cols) - 1:
			print(cols[i])
		else:
			print(cols[i], end=', ')
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
	else:
		query_table = ans
	for r in query_table:
		for i in range(len(r)):
			if i == len(r) - 1:
				print(r[i])
			else:
				print(r[i], end=', ')
