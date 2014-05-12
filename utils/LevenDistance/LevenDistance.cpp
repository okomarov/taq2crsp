////////////////////////////////////////////////// Levenshtein Distance //////////////////////////////////////////////////
// Compile this cpp to dll in Matlab:
// Preprocess:// Type "mex -setup"(not include the quotes) 
// in the command of Matlab,// you will be asked a Question, pls input 'y',
// next, select a compiler in your computer.// last, verify the information, pls input 'y'.
//
// Compile:
// put this cpp in your work directory. Then
// type "mex LevenDistance.cpp"(not include the quotes)
// a LevenDistance.dll file will be created 
// in the same folder.
//
// Then this function can be used.//
///////// Algorithm designed by: Michael Gilleland//
// C++ code for Matlab coded by: Longfei Ma 
// malf1988@gmail.com
// 2009.10.31 
///////////////////////////////////////////////
#include <string.h>
#include <malloc.h>
#include <mex.h> 
// the mex.h must be included
int LD (char const *, char const *);
//Matlab main function
void mexFunction(int nlhs,mxArray *plhs[],int nrhs,const mxArray *prhs[]){ 
    double *a; 
    char *s0,*s1;
    if(nrhs!=2)
    {
        mexErrMsgTxt("You should input TWO string argument, \nPls check the input."); 
        return; 
    }
    plhs[0]=mxCreateDoubleMatrix(1,1,mxREAL);//create output argument 
    a=mxGetPr(plhs[0]); 
    int len0,len1; len0 = (mxGetM(prhs[0]) * mxGetN(prhs[0])) + 1;//get the length of the input string 
    len1 = (mxGetM(prhs[1]) * mxGetN(prhs[1])) + 1; 
    s0=(char *)mxCalloc(len0,sizeof(char));//allocate memory for storing 
    s1=(char *)mxCalloc(len1,sizeof(char)); 
    mxGetString(prhs[0],s0,len0);//put the input string into s0 
    mxGetString(prhs[1],s1,len1); 
    *a=LD(s0,s1);}
//****************************// Get minimum of three values//****************************
int Minimum (int a, int b, int c)
{
    int mi; mi = a; if (b < mi) { mi = b; } if (c < mi) { mi = c; } return mi; 
}
//**************************************************
// Get a pointer to the specified cell of the matrix
//************************************************** 
int * GetCellPointer (int *pOrigin, int col, int row, int nCols)
{ return pOrigin + col + (row * (nCols + 1));}
//*****************************************************
// Get the contents of the specified cell in the matrix 
//*****************************************************
int GetAt (int *pOrigin, int col, int row, int nCols)
    { int *pCell; pCell = GetCellPointer (pOrigin, col, row, nCols); return *pCell; }
//*******************************************************
// Fill the specified cell in the matrix with the value x
//*******************************************************
void PutAt (int *pOrigin, int col, int row, int nCols, int x)
{ 
    int *pCell; 
    pCell = GetCellPointer (pOrigin, col, row, nCols); *pCell = x; 
}
//*****************************// Compute Levenshtein distance//*****************************
int LD (char const *s, char const *t)
{
    int *d; // pointer to matrix 
    int n; // length of s 
    int m; // length of t 
    int i; // iterates through s 
    int j; // iterates through t 
    char s_i; // ith character of s 
    char t_j; // jth character of t 
    int cost; // cost 
    int result; // result 
    int cell; // contents of target cell 
    int above; // contents of cell immediately above 
    int left; // contents of cell immediately to left 
    int diag; // contents of cell immediately above and to left 
    int sz; // number of cells in matrix 
// Step 1 
    n = strlen (s); 
    m = strlen (t); 
    if (n == 0) 
    {
        return m; 
    } 
    if (m == 0) 
    { 
        return n; 
    }
    sz = (n+1) * (m+1) * sizeof (int); 
    d = (int *) malloc (sz); 
// Step 2 
    for (i = 0; i <= n; i++) 
    {
        PutAt (d, i, 0, n, i); 
    } 
    for (j = 0; j <= m; j++) 
    { 
        PutAt (d, 0, j, n, j); 
    } 
// Step 3 
    for (i = 1; i <= n; i++) 
    { 
        s_i = s[i-1]; 
// Step 4 
        for (j = 1; j <= m; j++) 
        { 
            t_j = t[j-1]; 
// Step 5
            if (s_i == t_j) 
            { 
                cost = 0; 
            } 
            else 
            { cost = 1; 
            } // Step 6 
            above = GetAt (d,i-1,j, n); 
            left = GetAt (d,i, j-1, n); 
            diag = GetAt (d, i-1,j-1, n); 
            cell = Minimum (above + 1, left + 1, diag + cost); 
            PutAt (d, i, j, n, cell); 
        } 
    } 
// Step 7 
    result = GetAt (d, n, m, n); free (d); return result; 
}
