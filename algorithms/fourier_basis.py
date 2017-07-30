import np
import math
def real_dft_matrix(n):
    W = [half_alternate(n, i) for i in range(n+1)]
    return W
def ortho_real_dft(n):
    W = [alternate(n, i) for i in range (0, 2*n)]
    return W
def complex_dft_matrix(n):
    real = [cos_atom(n,i) for i in range(n)]
    comp = [sin_atom(n,i) for i in range(n)]
    comb = [zip(real[i],comp[i]) for i in range(n)]
    def complexify(pair):
        return pair[0]+1j*pair[1]
    def c_pairs(lst):
        return [complexify(p) for p in lst]
    return [c_pairs(lst) for lst in comb]
def row_normed(nested_lst):
    return [atom_norm(i) for i in nested_lst]
def atom_norm(atom):
    return list(np.Vector(*atom).zero_mean_normalize())
def zmean_real_dft(n):
    W = real_dft_matrix(n)
    return W[1::]
def zmean_ortho_dft(n):
    W = ortho_real_dft(n)
    return W
def bp_fourier(n):
    W = zmean_real_dft(n)
    A = t(row_normed(W[1::]))
    return A
def alternate(N, idx):
    f = idx%N
    cos = idx<N
    return cos_atom(N, f) if cos else sin_atom(N,f)
def half_alternate(N, idx):
    f = idx//2
    even = idx%2 ==0
    return cos_atom(N,f) if even else sin_atom(N,f)
def cos_atom(N,f):
    scale = 1/math.sqrt(N)
    return [scale*math.cos(2*math.pi*f*i/N) for i in range(N)]
def sin_atom(N, f):
    scale = -1/math.sqrt(N)
    return [scale*math.sin(2*math.pi*f*i/N) for i in range(N)]
def transp(mat):
    """psueudo inverse of the full (including DC) fourier matrix"""
    N = len(mat[0])
    mat[0] = lst_mult(0.5, mat[0])
    mat[-1] = lst_mult(0.5, mat[-1])
    scaled =  el_mult((2./N), t(mat))
    return scaled

def inv_dft_mat(len_spectrum):
    len_signal = len_spectrum+1
    return t(bp_fourier(len_signal))

def imagify(tpl):
    return (tpl[0]+tpl[1]*1j)

def package(result):
    n = len(result)//2
    zipped = zip(result[0:n], result[n:2*n])
    return [imagify(pair) for pair in zipped]

def half_package(result):
    n = len(result)//2
    zipped = zip(result[0::2], result[1::2])
    return [imagify(pair) for pair in zipped]


def t(l):
    return [list(i) for i in zip(*l)]

def normed(eg_array):
    return (eg_array - eg_array.mean(axis=0)) / np.linalg.norm(eg_array, axis = 0)

def bp_ft(signal):
    A = bp_fourier(len(signal))
    #sol = list(solve_basis_pursuit(A,signal))[-1][1]
    sol = sk_bp(A, signal).coef_
    return list(sol)

def lst_mult(factor, lst):
    return [factor*i for i in lst]

def el_mult(factor, lst_lists):
    return [lst_mult(factor, i) for i in lst_lists]