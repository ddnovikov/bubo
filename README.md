# bubo

Triangular arbitrage bot for crypto.com written using `tokio`.

# How to run

`cargo run --release`

# General description

1. An implementation of triangular arbitrage on crypto.com exchange.
2. The logic is pretty straightforward and is more or less easy to quickly change to test your hypothesis.
3. Bot is resource-efficient and fast enough to adapt to market changes (tested on a machine with 1 CPU and 500 MB RAM, consumes ~2% of CPU and tens of MBs of RAM at most).
4. Doesn't generate profit on the current state of the market and mostly loses money slowly, but can provide insights and technical solutions to the problems that one has to solve anyway to make this kind of software.
5. May have some non-critical bugs.
6. May have minor dumb solutions which I was too lazy to fix when I got bored to work on the bot.

# My experience

I've researched a decent amount of materials on the triangular arbitrage. You might know or have a feeling that there is not a lot of quality content on this topic - and you are right. This is the rare case of the business field where the only way to test your ideas is to actually implement them.
There are still some research papers and articles on the topic. Unfortunately, most of them are out of touch with the realities of trading. This is a common illness for a lot of research papers, and arbitrage is not an exception - the most of those materials are about solving a task of finding arbitrage opportunities by representing an exchange as a graph, solving a problem of detecting the negative cycles on it, etc., etc. But in practice it turns out that writing a brute-force solution and trying to optimize it is better for a few reasons:
1. Finding arbitrage oppotunities is not the main problem if you want to write a program that earns money. Triangular arbitrage for an individual chain  is a few multiplications and divisions which can be done very fast.
2. Thus, you can lower the complexity of your program by optimizing a brute-force approach, which is advanced-grade problem (at least for me as a guy who had no prior experience in writing async applications).
3. I solved item 2 by applying a "funneling" approach to do chain computations, i.e. reducing useless actions to a minimum on each logical stage of chain discovery and execution: a) getting rid of the instruments we don't want to trade anyway; b) spawning a task per chain and dispatching each websocket update so it affects only the chains (tasks) that have this instrument; c) tuning tasks yields to make sure we don't cause undesired thread blocks or something like that (`tokio-console` is very helpful here); d) other minor tweaks similiar to aforementioned ones.
4. Another problem that comes up frequently is being fast enough to place an order with the correct price point (definitely less than 30 ms, but I didn't do any performance tests). In my case, this was solved by doing all of what's described in item 3.
5. Taking item 4 into account I should also mention that even if fancy graph-based arbitrage discovery algorithm provides a benefit of e.g. finding the best chain on the whole exchange - it may be way slower than needed to actually make an order that at least has a correct price point that an exchange is going to match.

# The Real Problem

The really hard part of writing such bot is a problem of liquidity and trading volumes. If you write a correct arbitrage discovery algorithm - you'll be able to see quite a decent amount of opportunities with nice gains in the range of 1.001 to 1.009 in most cases, sometimes going even up to 1.02. But the reality is that those opportunities are "fake" because almost all of them are not fully executable (i.e. you can't reliably execute all the deals in the chain). What's happening here (to my understanding): low liquidity means that it's hard to trade a currency and instrument has a low 24h volume under 50K dollars => people trade this cryptocurrency less and less => this causes price imbalance like XXX_USDT or XXX_USDC having way bigger volume than XXX_BTC and thus you can buy more XXX for BTC than to same amount of USDT or USDC => our program sees this price difference and tries to execute any programmed limit order with such instruments => FILL_OR_KILL orders are canceled due to inability to fill even 1-2 dollars order; IMMEDIATE_OR_CANCEL are either canceled or partially filled with 2-10% fill rate; GOOD_TILL_CANCEL hang around infinitely (so it's just easier to use IMMEDIATE_OR_CANCEL instead). Amount of opportunities with instruments of high liquidity is close to zero, which I suspect to be natural as the price leveling between e.g. BTC_USDT and BTC_USDC happens instantly due to huge amounts of various orders being filled every second.

# Conclusions

My experience with this bot (this is a third attempt by the way) leads me to these final thoughts:

1. Arbitrage opportunities are caused by market imbalances, which is basically an asset having a correct price point against one commodity and incorrect against the other (being over- or under-appreciated). To do arbitrage, you need to execute a set of orders, which require liquidity to be filled. Where liquidity is present, bigger volumes are traded, bid-ask spread is less and the price is the most fair. *My hypothesis is: to do arbitrage successfully, you need liquidity that is high enough to fill your orders, but low enough to allow price imbalances.* I suppose that to catch those situations you need the market to be bullish, as more volume and more liquidity is being introduced, thus allowing the symbols with low volumes provide sufficient liquidity to do the arbitrage while still being not liquid enough to get rid of price imbalances.
2. The huge question that is yet to be answered is whether doing arbitrage even in the bullish market is going to outperform hodling a portfolio of the top cryptocurrencies. I have a suspicion that this might not be the case.
3. One should not forget about the big guys with big money co-locating with the exchanges to get exclusive trading opportunities. They may fill the arbitrage gaps faster than we can see them through the public websockets.
4. I think that such a bot should be a small program with very thin abstractions that are unavoidable. Exchanges' APIs have a lot of weird behaviours and oftentimes do not work as explained in the documentation, thus trying to come up with architecture that can handle all of that will take a lot of time that you could've spent testing your ideas. Most of the time of 2 months spent on this bot I was testing one hypothesis or another. Imagine how much more time I would've spent to come to the same conclusions which seems to be common for the whole crypto market.

# Notes

1. Look for comments starting with "DD:" (design decision) to check out the details of the implementation.
2. Executing orders simultaneously is more effective than doing them sequentially, because the price updates received in between the executed orders are usually showing that the opportunity is gone. You can do this yourself if you are interested enough :)
3. I've put quite an effort to this bot and the main functionality works fine and fast enough. I got tired of not having financial gains with it so I was too lazy to polish the final result. So, please don't fall into the nerd rage if you see some imperfections :)
4. Take my experience with a healthy bit of skepticism and think for yourself. Maybe you'll be more smart or lucky than me if you give it a try. Good luck!
