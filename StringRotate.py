#!/usr/bin/python3
def rotate_string(string, direction, n)
	total = len(string)
	if n le total
		total % n
		if direction = left
			return string[n]:[total] + string[0]:[n]
		elif direction == right
			return string[0]:[-n] + string[-n]:[total]
		else:
			raise ErrorStatement
	else:
		return ErrorStatement

#Original request: Given a string, a direction, and an integer, rotate the string the given number of times in the given direction
