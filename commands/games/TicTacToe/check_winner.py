# Setting up logging
import logging

from discord.ui import View

from utilities.logger_setup import get_logger

logger = get_logger("CheckWinner")



def check_winner(board):
    size = len(board)  # Determine the size of the board (e.g., 3x3 or 5x5)

    # Check rows for a winner
    for row in board:
        if row.count(row[0]) == size and row[0] != " ":
            logger.info(f"Row winner found: {row[0]}")
            return row[0]

    # Check columns for a winner
    for col in range(size):
        column = [board[row][col] for row in range(size)]
        if column.count(column[0]) == size and column[0] != " ":
            logger.info(f"Column winner found: {column[0]}")
            return column[0]

    # Check main diagonal
    if all(board[i][i] == board[0][0] and board[0][0] != " " for i in range(size)):
        logger.info(f"Main diagonal winner found: {board[0][0]}")
        return board[0][0]

    # Check anti-diagonal
    if all(board[i][size - i - 1] == board[0][size - 1] and board[0][size - 1] != " " for i in range(size)):
        logger.info(f"Anti-diagonal winner found: {board[0][size - 1]}")
        return board[0][size - 1]

    return None