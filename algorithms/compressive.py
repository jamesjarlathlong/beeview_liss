import scipy.fftpack as spfft
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
import fourier_basis as ft
import cvxpy as cvx
import itertools
import math
import numpy as np
import random
import sys
import fourier_basis as ft

def downsample(data,ri):
    return data[ri]
def reconstruct(downsampled, basis, ri):
    A = basis[ri]
    n =len(basis)
    vx = cvx.Variable(n)
    objective = cvx.Minimize(cvx.norm(vx, 1))
    constraints = [A*vx == downsampled]
    prob = cvx.Problem(objective, constraints)
    result = prob.solve(verbose=False)
    return vx
def fourier_denoising(lamda,downsampled, basis, ri):
	A = basis[ri]
	n =len(basis)
	w = cvx.Variable(n)
	loss = cvx.sum_squares(A*w-downsampled)/2 + lamda * cvx.norm(w,1)
	problem = cvx.Problem(cvx.Minimize(loss))
	result = problem.solve(verbose=True) 
	return w
def freq_and_time(A, ri, signal, method):
    vx = method(signal[ri], A, ri)
    x = np.array(vx.value)
    x = np.squeeze(x)
    sig = np.dot(A,x)
    return x, sig
def fourier_cs(signal, downsample_factor, method=reconstruct):
	n = len(signal)
	m =int(n//downsample_factor)
	A = np.array(ft.t(ft.zmean_real_dft(n)))
	ri = np.random.choice(n, m, replace=False) # random sample of indices
	f, t = freq_and_time(A, ri, signal, method=method)
	return f,t


if __name__ == "__main__":
	data = np.loadtxt(sys.argv[1])
	factor = int(sys.argv[2])