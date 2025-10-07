

# Dictionary to store game state per channel
hangman_games = {}

# Hangman visual stages
HANGMAN_STAGES = [
    "----\n|\n|\n|",  # Initial empty structure
    "----\n|   O\n|\n|",  # Add the head
    "----\n|   O\n|   |\n|",  # Add the body
    "----\n|   O\n|  /|\n|",  # Add one arm
    "----\n|   O\n|  /|\\\n|",  # Add the second arm
    "----\n|   O\n|  /|\\\n|  /",  # Add one leg
    "----\n|   O\n|  /|\\\n|  / \\",  # Add the second leg (complete figure)
]

# List of words for random selection
WORD_LIST = ["python", "developer", "discord", "interaction", "programming"]