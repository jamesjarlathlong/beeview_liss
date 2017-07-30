"""
we're trying to solve the problem
min ||x||_1 st Ax=b where b is the the signal
A is a basis matrix and x is the solution vector
"""
import math
import np
import coord_descent as cd
import fourier_basis as ft

def sk_bp(A, signal, alpha):
    clf = cd.Lasso(alpha=alpha, max_iter=100)
    fitted = clf.fit(A, signal)
    return fitted.coef_
def sparseft(signal, alpha=1e-4):
	N = len(signal)
	W = ft.bp_fourier(N)
	v = np.Vector(*signal).zero_mean_normalize()
	spectrum = sk_bp(W, v, alpha)
	return spectrum
def sparse_rep(spectrum):
	return {idx+1:val for idx,val in enumerate(spectrum) if val}
def inverse_ft(spectrum):
	N = len(spectrum)+1
	W = ft.bp_fourier(N)
	vec = np.Vector(*spectrum)
	return vec.matrix_mult(W)
def sparseinverseft(spectrum_dictionary, N):
	def unroll(dct, n):
		return (dct.get(i,0) for i in range(n-1))
	vec = unroll(spectrum_dictionary, N)
	return inverse_ft(vec)
def sft(signal):
	N = len(signal)
	W = ft.bp_fourier(N)
	v = np.Vector(*signal)
	return v.matrix_mult(W)
def isft(spectrum):
    inverse_W = ft.inv_dft_mat(len(spectrum))
    v = np.Vector(*spectrum)
    return spectrum.matrix_mult(inverse_W)
	