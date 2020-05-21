#This file houses utility functions for finding outputs that aren't essential to PE calculations, but make
#outputs more readable, and hopefully easier for the public to understand what goes on under the hood

import numpy as np
from Separator import  indicesToStartEnd
def addToSourceText(starts, ends, texts, sourceText):
    #print(texts)
    for i in range(len(starts)):
        pointer = 0
        myText = texts[i]


        for c in range(starts[i], ends[i]):
            #print(starts[i], ends[i])
            sourceText[c] = myText[pointer]
            pointer += 1
    return sourceText

def getTextFromIndices(indices,  sourceText):
    out = ''
    #print(sourceText)
    starts, ends, chunks = indicesToStartEnd(indices)
    for i in range(len(starts)):
       start = starts[i]
       end = ends[i]
       for l in range(start, end+1):

          if type(sourceText[l]) == str:
             #print(sourceText[l])
             out+=sourceText[l]
          else:
              print(sourceText[l],'foundzerochar', l)

       out+= "//break//"
    assert out.count("//break//") == len(starts)
    print(out.count("//break//"))
    return out
def makeList(size):
    out = []
    for i in range(size):
        out.append(0)
    return out

def getText(start,end, sourceText):
    out = ''
    for i in range(start,end):
        out = out+sourceText[i]
    return out

def oddsDueToChance(percentage, num_users = 5, num_choices = 2):
    """Simulates the odds that agreement is due to chance based off of a coinflip"""
    #print('simulating chance')
    return .5
    if percentage<.5:
        return 1
    percentages = np.zeros(0)
    for i in range(10000):
        flips = np.random.choice(num_choices, num_users)
        for choice in range(num_choices):
            counts = []
            counts.append(np.sum(flips == choice))
        highCount = max(counts)
        highScore = highCount/num_users
        percentages = np.append(percentages, highScore)
        #print(flips, counts, highCount,highScore, percentages)
    winRate = np.sum(percentages>=percentage)/10000
    #print('Done')
    return winRate