"""
we're trying to solve the problem
min ||x||_1 st Ax=b where b is the the signal
A is a basis matrix and x is the solution vector
"""
import simplex
def obj_variables(signal):
	n = len(signal)
	return ('x_'+str(i+1) for i in range(2*n))
def get_coef(n, varname):
	varnum = varname.split('_')[1]
	return str(1) if int(varnum) <= n else str(0)

def create_objective_string(the_vars):
	"""given our signal and our basis matrix
	return the objective function string 
	to be fed into the simplex solver
	string is of the form '4x_1+2_x2...'"""
	n = len(the_vars)//2
	with_coef = (get_coef(n,v)+v for v in the_vars)
	together = (' + ').join(with_coef)
	return together
def flatten(gen):
	return (x for y in gen for x in y)
def create_l1_constraints(the_vars):
	"""for every var in the objective function we need
	to add a constraint for the actual solution var.
	e.g. s1<=x1, s1>=-t1"""
	pairs = get_pairs(the_vars)
	l1_constraints = flatten((get_l1_constraint(pair) for pair in pairs))
	return list(l1_constraints)
def get_pairs(the_vars):
	n = len(the_vars)//2
	return zip(the_vars[n:], the_vars[0:n])
def get_l1_constraint(pair):
	yield diff(stringify(pair))
	yield summ(stringify(pair))
def stringify(pair):
	return [str(1)+p for p in pair]
def diff(pair):
	return ' - '.join(pair)+ ' 1 <= 1'
def summ(pair):
	return ' + '.join(pair)+ ' 1 >= 1'

def combine_coef_var(tupl):
	return str(tupl[0])+tupl[1]
def dotvars(cv_pairs):
	combined = (combine_coef_var(pair) for pair in cv_pairs)
	return ' + '.join(combined)
def zipdot(A, y):
	return (zip(row, y) for row in A)
def full_eq(sides):
	return ' = '.join(sides)
def create_basis_constraints(signal, A, the_vars):
	"""given our signal and our basis matrix
	return the list of constraint strings
	to be fed into the simplex solver
	constraint string is of the form '3x_1 + 1x_2 = 3'
	A is a list of lists(rows)"""
	n = len(signal)
	y = the_vars[n:]
	lhsides = (dotvars(pairs) for pairs in zipdot(A, y))
	rhsides = (str(i) for i in signal)
	return [full_eq(two_sides) for two_sides in zip(lhsides, rhsides)]
def solve_basis_pursuit(A, signal):
	the_vars = list(obj_variables(signal))
	obj_fun = create_objective_string(the_vars)
	l1s = create_l1_constraints(the_vars)
	equalities = create_basis_constraints(signal, A, the_vars)
	all_constraints = l1s+equalities
	res, sol = linsolve(objective, ineq_left = ineq_left, ineq_right = ineq_right,
						 eq_left = eq_left, eq_right = eq_right, nonneg_variables = dummies)
	return sol
if __name__ == '__main__':
	signal = [1, 2, 15]
	A = [[11,12,13]
		,[21,22,23]
		,[31,32,33]]
	solution = solve_basis_pursuit(A, signal)
	print(Lp_system.solution)
	print(Lp_system.optimize_val)


	