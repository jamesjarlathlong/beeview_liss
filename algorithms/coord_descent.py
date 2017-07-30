import np
from copy import deepcopy
import collections
def t(l):
    return [list(i) for i in zip(*l)]
def memoize(function):
	memo = {}
	def wrapper(*args):
		if args[0] in memo:
			return memo[args[0]]
		else:
			rv = function(*args)
			memo[args[0]] = rv
			return rv
	return wrapper
def purge(d, n):
	for i in range(n):
		d.popitem(last = False)
	return d
def tuple_memoize(function):
	meta_memo = {}
	memo = {}
	def wrapper(*args):
		_id = frozenset(args[0])
		idxs = frozenset(pair[0] for pair in _id)
		fmatted = sorted([i for i, val in _id])
		if _id in memo:
			return memo[_id]
		else:
			rv = function(*args)
			if idxs in meta_memo:
				previous_id = meta_memo[idxs]
				del memo[previous_id]
			meta_memo[idxs] = _id#we want to overwrite to save space
			memo[_id] = rv
			return rv
	return wrapper
@tuple_memoize
def summer(idx_vals, idx_cols):
	idxs = [i[0] for i in idx_vals]
	if len(idxs) == 1:
		return np.Vector(*idx_cols[idxs[0]]) 
	else:
		mutate_idxs = list(idx_vals)
		tail = mutate_idxs[-1][0]
		head = tuple(mutate_idxs[:-1])
		return np.Vector(*idx_cols[tail]) + summer(head, idx_cols)
@tuple_memoize
def group_summer(idx_vals, idx_cols):
	n = len(idx_vals)
	if n <=50:
		return intra_summer(idx_vals, idx_cols)
	else:
		mutate_idxs = list(idx_vals)
		tail = mutate_idxs[n-50:]
		head = tuple(mutate_idxs[:-50])
		return intra_summer(tail, idx_cols) + group_summer(head, idx_cols)
def intra_summer(idx_vals, idx_cols):
	all_cols = (idx_cols[v[0]] for v in idx_vals)
	summed_cols = np.Vector(*(sum(i) for i in zip(*all_cols)))#
	return summed_cols
@memoize
def row_multer(zipped_idx_val, XT):
	idx, val = zipped_idx_val
	return [val*mat_val for mat_val in XT[idx]]

def get_active(lst):
	return [(idx,v) for idx, v in enumerate(lst) if v]
class Lasso():
	def __init__(self, alpha=1.0, max_iter=1000):
		self.alpha = alpha
		self.max_iter = max_iter
		self.coef_ = None
		self.intercept_ = None
		self.counter = 0
	def _soft_thresholding_operator(self, x, lambda_):
		if x > 0 and lambda_ < abs(x):
			return x - lambda_
		elif x < 0 and lambda_ < abs(x):
			return x + lambda_
		else:
			return 0
	def step(self, X, XT, beta, y, j):
		tmp_beta = np.change_idx(beta, j, 0.0)
		r_j = y - self.cached_mult(tmp_beta, XT, recursive=True)
		arg1 = r_j.inner(XT[j])
		arg2 = self.alpha*len(X)
		new_beta_j = self._soft_thresholding_operator(arg1, arg2)/sum(np.Vector(*XT[j])**2)
		return np.change_idx(beta, j, new_beta_j)
	def cached_mult(self,tmp_beta, XT, recursive=False):
		active_coefs = get_active(tmp_beta)
		if active_coefs:
			if recursive:
				all_cols = dict(((zipped_idx_val[0],row_multer(zipped_idx_val,XT))
								 for zipped_idx_val in active_coefs))
				idxs = tuple(active_coefs)
				summed_cols = group_summer(idxs, all_cols)
			else:
				all_cols = (row_multer(zipped_idx_val,XT) for zipped_idx_val in active_coefs)
				summed_cols = np.Vector(*(sum(i) for i in zip(*all_cols)))#
			return summed_cols
		else:
			dummy_lst = len(XT[0])*[0]
			summed_cols = np.Vector(*dummy_lst)
		return summed_cols

	def fit(self, X, y):
		cols = len(X[0])
		rows = len(X)
		beta = np.Vector(*cols*[0])
		iteration = 0
		XT = t(X)
		while iteration<self.max_iter:
			old_beta = np.Vector(*beta)
			for j in range(len(beta)):
				beta = self.step(X, XT, beta,y, j)
			iteration+=1
			if converged(beta, old_beta):
				break
		self.coef_ = beta
		return self

def converged(beta_new, beta):
	norm_dif  = (beta_new - beta).norm()
	print('norm diff: ', norm_dif)
	return (norm_dif/(beta.norm()+1e-9) < 1e-9)


