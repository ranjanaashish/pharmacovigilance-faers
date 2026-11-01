# Pharmacovigilance Signal Detection: PRR Methodology

## 1. Background & Epidemiological Context

**Pharmacovigilance (PV)** is defined by the World Health Organization (WHO) as the science and activities relating to the detection, assessment, understanding, and prevention of adverse effects or any other drug-related problem.

While randomized controlled trials (RCTs) establish drug efficacy and common short-term safety profiles prior to market approval, they are inherently limited by:
- Constrained sample sizes ($n \approx 1,000 - 5,000$ patients)
- Highly restricted enrollment criteria (excluding children, pregnant women, elderly, and polymorbid patients)
- Limited follow-up durations (rarely exceeding 1–3 years)
- Statistical inability to detect rare adverse drug reactions (ADRs) occurring at frequencies $< 1 \text{ in } 10,000$.

Post-marketing spontaneous reporting systems (SRSs), such as the **FDA Adverse Event Reporting System (FAERS)** with $> 16\text{ million}$ reports, constitute the bedrock of global drug surveillance. However, spontaneous reporting suffers from lack of an exposed denominator (true drug utilization figures are unknown) and voluntary reporting biases. To overcome these constraints, regulatory authorities and epidemiologists utilize **Disproportionality Analysis**.

---

## 2. The 2×2 Contingency Table

Disproportionality analysis models adverse event reports by comparing the observed proportion of a specific reaction among reports for a target drug against the proportion of the same reaction across all other drugs in the reference database.

For a specific **Drug $X$** and adverse **Reaction $Y$**, the universe of primary suspect reports ($N$) is partitioned into a $2 \times 2$ contingency table:

```
                      Reaction Y (Target ADR)    All Other Reactions (Not Y)         Marginal Total
+-------------------+-------------------------+-------------------------------+--------------------------+
| Drug X (Target)   |            a            |               b               |    a + b  (drug_total)   |
+-------------------+-------------------------+-------------------------------+--------------------------+
| All Other Drugs   |            c            |               d               |    c + d                 |
+-------------------+-------------------------+-------------------------------+--------------------------+
| Marginal Total    |     a + c (rxn_total)   |             b + d             |    N = a + b + c + d     |
+-------------------+-------------------------+-------------------------------+--------------------------+
```

### Cell Definitions:
- **$a$**: Number of reports mentioning **Drug $X$** with **Reaction $Y$** (co-occurrence count).
- **$b$**: Number of reports mentioning **Drug $X$** with any reaction **other than Reaction $Y$**.
- **$c$**: Number of reports mentioning any drug **other than Drug $X$** with **Reaction $Y$**.
- **$d$**: Number of reports mentioning any drug **other than Drug $X$** with any reaction **other than Reaction $Y$**.
- **$a + b$**: Total reports in the database where **Drug $X$** is the suspect medication.
- **$a + c$**: Total reports in the database where **Reaction $Y$** is documented.
- **$N$**: Total distinct adverse event reports evaluated in the surveillance dataset ($N = a + b + c + d$).

---

## 3. Mathematical Formulation

### 3.1 Proportional Reporting Ratio (PRR)

Introduced by Evans, Waller, and Davis (2001) at the UK Medicines Control Agency (now MHRA), the **Proportional Reporting Ratio (PRR)** evaluates the ratio of the reporting rate of Reaction $Y$ for Drug $X$ relative to the reporting rate of Reaction $Y$ for all other medications:

$$\text{PRR} = \frac{\text{Proportion of Reaction } Y \text{ among reports for Drug } X}{\text{Proportion of Reaction } Y \text{ among reports for Other Drugs}} = \frac{\frac{a}{a + b}}{\frac{c}{c + d}}$$

Algebraically rearranging into cross-products:

$$\text{PRR} = \frac{a \cdot (c + d)}{c \cdot (a + b)}$$

### 3.2 Interpretation of PRR
- **$\text{PRR} = 1.0$**: Null hypothesis. The adverse reaction occurs at exactly the background frequency expected across the pharmacovigilance database.
- **$\text{PRR} < 1.0$**: Inverse reporting. Reaction $Y$ is reported less frequently with Drug $X$ than with other medications.
- **$\text{PRR} > 1.0$**: Positive disproportionality. Reaction $Y$ is reported disproportionately more often with Drug $X$.

### 3.3 Variance and 95% Confidence Intervals

The logarithm of PRR is asymptotically normally distributed. The standard error of $\ln(\text{PRR})$ is estimated as:

$$\text{SE}(\ln(\text{PRR})) = \sqrt{\frac{1}{a} - \frac{1}{a + b} + \frac{1}{c} - \frac{1}{c + d}} = \sqrt{\frac{b}{a(a+b)} + \frac{d}{c(c+d)}}$$

The 95% Confidence Interval for PRR is calculated as:

$$\text{PRR}_{95\% \text{ CI}} = \left[ \exp\left( \ln(\text{PRR}) - 1.96 \cdot \text{SE} \right), \; \exp\left( \ln(\text{PRR}) + 1.96 \cdot \text{SE} \right) \right]$$

---

## 4. Statistical Significance: Pearson Chi-Square ($\chi^2$)

To guard against spurious signals generated by high variance in sparse contingency cells (e.g., $a = 1, b = 0$), the PRR point estimate is paired with the Pearson Chi-Square test of independence (with 1 degree of freedom):

$$\chi^2 = \frac{N \cdot (a \cdot d - b \cdot c)^2}{(a + b) \cdot (c + d) \cdot (a + c) \cdot (b + d)}$$

Under the null hypothesis of independence between Drug $X$ and Reaction $Y$, $\chi^2$ follows a chi-square distribution with $\text{df} = 1$.
- A threshold of $\chi^2 \ge 3.841$ corresponds to a critical $p$-value of $\alpha = 0.05$.
- In pharmacovigilance practice, the threshold is conservatively rounded to $\chi^2 \ge 4.0$ ($p \approx 0.0455$) to minimize false discovery rates.

---

## 5. WHO & Regulatory Signal Thresholds

This pipeline applies the internationally recognized **Evans et al. (2001)** / **WHO / EMA** triad criteria for automated signal flag declaration (`is_signal = True`):

$$\boxed{ \text{Signal Detected} \iff \Big( \text{PRR} \ge 2.0 \Big) \;\land\; \Big( a \ge 3 \Big) \;\land\; \Big( \chi^2 \ge 4.0 \Big) }$$

### Rationale for Triad Conditions:
1. **$\text{PRR} \ge 2.0$ (Clinical Effect Magnitude)**: The reporting proportion of the reaction is at least double the baseline reporting proportion of all other medications combined.
2. **$a \ge 3$ (Sample Stability)**: Eliminates single isolated anomalies ($a=1$ or $a=2$) that frequently arise from accidental reporting errors or idiosyncrasies.
3. **$\chi^2 \ge 4.0$ (Statistical Confidence)**: Ensures that the observed elevation is statistically significant at $p < 0.05$, ruling out chance fluctuations.

---

## 6. Real-World Case Benchmark: Rosiglitazone & Myocardial Infarction

A landmark historical case demonstrating the power of PRR signal detection is **Rosiglitazone (Avandia)** and **Myocardial Infarction (MI)** (Nissen & Wolski, 2007; Evans et al., 2001).

### Contingency Benchmark in FAERS Subsets:
- $a = 450$ (Rosiglitazone + MI)
- $b = 4,550$ (Rosiglitazone + other reactions) $\to a + b = 5,000$
- $c = 500$ (Other drugs + MI)
- $d = 109,500$ (Other drugs + other reactions) $\to c + d = 110,000$
- $N = 115,000$

$$\text{PRR} = \frac{450 / 5,000}{500 / 110,000} = \frac{0.0900}{0.004545} \approx \mathbf{19.8}$$

$$\chi^2 = \frac{115,000 \cdot (450 \cdot 109,500 - 4,550 \cdot 500)^2}{(5,000) \cdot (110,000) \cdot (950) \cdot (114,050)} \approx \mathbf{4,263.1} \gg 4.0$$

Both $\text{PRR} = 19.8 \ge 2.0$, $a = 450 \ge 3$, and $\chi^2 = 4,263.1 \ge 4.0$ were dramatically exceeded, generating a catastrophic safety alert that led to black box warnings and severe prescription restrictions globally.

---

## 7. Methodological Limitations & Biases in Pharmacovigilance

While PRR is computationally efficient and foundational for Big Data scanning across millions of rows in PySpark, epidemiologists must account for several inherent constraints:

### 7.1 Masking Effect (Competition Bias)
When a large number of reports for a particular high-volume adverse reaction (e.g., nausea or headache) flood the database, the total denominator for other reactions expands artificially. This can mask genuine signals for other drug-event pairs by artificially inflating $c$ or $d$.

### 7.2 Notoriety Bias & The Weber Effect
- **Notoriety Bias**: Media publicity or regulatory advisories regarding a specific drug-reaction association trigger spikes in reporting from clinicians and patients, inflating $a$ and generating artificially exaggerated PRR values.
- **Weber Effect**: Spontaneous reporting rates for a newly approved chemical entity typically peak during the second year of commercial release and decline steadily thereafter regardless of actual clinical incidence.

### 7.3 Under-Reporting
Only an estimated 5% to 10% of all real-world adverse drug events are ever reported to spontaneous reporting registries such as FAERS. The reporting rate depends heavily on reaction severity, patient anxiety, and clinician awareness.

### 7.4 Confounding by Indication
An apparent association between a drug and a reaction may reflect the underlying disease state for which the medication was prescribed rather than a true toxicological effect (e.g., antidiabetic medications showing statistical association with diabetic ketoacidosis or cardiovascular events).

### 7.5 Absence of True Denominator
Because spontaneous systems capture only adverse event reports and not total patient prescriptions or person-years of exposure, **PRR does NOT calculate true epidemiological incidence or absolute risk**. It is strictly a hypothesis-generating heuristic requiring confirmation via formal cohort or case-control studies.

---

## 8. Academic References

1. **Evans, S. J., Waller, P. C., & Davis, S. (2001).** Use of proportional reporting ratios (PRRs) for signal generation from spontaneous adverse drug reaction reports. *Pharmacoepidemiology and Drug Safety*, 10(6), 483-486.
2. **Rothman, K. J., Lanes, S., & Sacks, S. T. (2004).** The reporting odds ratio and its advantages over the proportional reporting ratio. *Pharmacoepidemiology and Drug Safety*, 13(8), 519-523.
3. **Bate, A., & Evans, S. J. (2009).** Quantitative signal detection using spontaneous ADR reporting. *Investigative Ophthalmology & Visual Science*, 50(6), 25-36.
4. **Nissen, S. E., & Wolski, K. (2007).** Effect of rosiglitazone on the risk of myocardial infarction and death from cardiovascular causes. *New England Journal of Medicine*, 356(24), 2457-2471.
5. **van Puijenbroek, E. P., et al. (2002).** A comparison of methods used for signal detection in spontaneous reporting systems. *Pharmacoepidemiology and Drug Safety*, 11(1), 3-10.
