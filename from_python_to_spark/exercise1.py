#!/usr/bin/env python
import sys

def simple_squares():
    numbers = [1,2,3,4,5]
    squares = []
    for number in numbers:
        squares.append(number*number)
        # Now, squares should have [1,4,9,16,25]
    print "List of squares: ", squares

def square(x):
    return x*x

def python_squares():
    ## Pythonic way
    numbers = [1,2,3,4,5]
    squares = map(square, numbers)
    #Now, squares should have [1,4,9,16,25]
    print "List of squares calculated in a Pythonic way: ", squares

def python_squares_lambda():
    ## Pythonic way
    numbers = [1,2,3,4,5]
    squares = map(lambda x: x*x, numbers)
    #Now, squares should have [1,4,9,16,25]
    print "List of squares calculated in a Pythonic way with lambda: ", squares

def filter_squares():
    numbers = [1,2,3,4,5]
    numbers_under_4 = []
    for number in numbers:
        if number < 4:
            numbers_under_4.append(number)
            # Now, numbers_under_4 contains [1,4,9]
    print "Numbers under 4 only: ",numbers_under_4


def python_filter_squares_lambda():
    numbers = [1,2,3,4,5]
    numbers_under_4 = filter(lambda x: x < 4,numbers)
    print "Numbers under 4 only: ",numbers_under_4


def main(args):
    #Exercise0: mapping the list
    simple_squares()

    # Mapping the list in a pythonic way
    #python_squares()

    #Lambda function 
    #python_squares_lambda()

    #Exercise1: filtering the list
    #filter_squares()

    #filtering the list in a pythonic way with lambda
    #python_filter_squares_lambda()

if __name__=='__main__':
    main(sys.argv)

