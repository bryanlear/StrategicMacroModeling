This repository houses a long-term project to model the global macroeconomic landscape as a strategic 'chess game' between the US and the Rest of the World (RoW). It involves collecting extensive US macroeconomic and market data (Treasuries, foreign bond holdings, financial assets) to gain a clear view of the 'board.' The ultimate goal is to better anticipate potential 'next moves' by leveraging concepts from Optimal Control Theory, Game Theory, Theory of Combat, and Multi-Agent Reinforcement Learning.

![Game Screenshot](images/game.jpg)


###### Definition #######


### players:
(aim: to gauge 'financial asset value' of each player)

* US 

* aggregated opponent ''Rest of the World'' RoW (Japan, UK, EU, China): define aggregation method --> single POMDP agent

### state space:

Partially Observable MDP: Markovian but with element of uncertainty about true state.

Players do not see true state s but observations o ∈ Ω
Players operate based on belief state b(s) = likelihood of system being in s. 

b(s) = Pr() distribution over all states

* US indicators 
 - economic output & growth
 - prices & inflation
 - labour market
 - ineterest rates 
 - fiscal health
 - trade & international position
 - currency strength 
 - market sentiment & stability
 - investment 
 - supply chain

Noisy, delayed, incomplete proxies for economic health (TRUE state)

* RoW
 - Foreign holdings
 - relative economic performance
 - global factors

* relational and global indicators

### action space:
(available levers players can pull) 

* US actions: 
 - fiscal policy 
 - monetary policy 
 - trade policy 

* aggregated:
  - capital allocation decisions (1) or reflective aggregated policy stance (2)
  - actions as reactions based on 2 (analogous to US actions)


### reward function
 
 - US: Maximize custom index "US Financial Asset Value" (UFAV).
   to consider components for constructing index. (+weights GDP, growth, SP500, low unemployment, rate of change inflation, demand for US assets, FX. -weights debt-to-GDP, VIX,...)

(to consider for secondary objective:

e.g.:

achieve sustainable GDP growth, maintain inflation around target, maintain low unemployment...)

 - RoW: Maximize custom aggregated index "RoW Economic Value" (RoWEV).


### dynamics & timing

CORE Challenge
transition probabilities T(s'|s,a)  

How does policy clashes affect state transitions? --> theory of combat

O(o∣s,a) Pr() of observing o when s' after action a is taken and transition is completed. 
(to consider, quality, timeliness of data)

 - Transition dynamics:

 - time steps (discrete time steps)
 - turns (simultaneous moves)
 - information 
 - horizon (tactical moves or long-term strategic positioning?)

### policy
π(b)

belief states to actions (optimal policy π^*)
to define: belief update mechanism (Bayes' rule)