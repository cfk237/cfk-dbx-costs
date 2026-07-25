# Report Spec — Databricks Costs

## Report identity
- **Report name:** Databricks Costs
- **Semantic model:** `fabric/Databricks Costs.SemanticModel`
- **PBIP project:** `fabric/Databricks Costs.pbip`
- **Audience:** IT Platform / FinOps team
- **Primary purpose:** Track, attribute, and optimise Databricks spend across workspaces, resources, and identities
- **Delivery target:** Local PBIP only (no Fabric publishing)

---

## User decisions and constraints
- **Scope:** 3-page FinOps report — Cost Overview, Cost Attribution, Resource Detail
- **Page count:** 3 visible pages
- **Interactivity:** Date range slicer, workspace slicer, resource type slicer, identity type slicer; cross-filter between visuals; drill-filter enabled
- **Design direction:** Brand-forward (Ipsen) — teal primary, dark navy header band, white canvas, Ipsen logo on every page
- **Publishing:** None — local PBIP only
- **Tooling:** powerbi-modeling-mcp (connected), TMDL file edits for model changes
- **Model edit permissions:** Allowed — 2 new measures required
- **Accessibility:** WCAG AA contrast minimum; alt text on all charts; searchable slicers for high-cardinality fields
- **Data caveats:** `Top 10 Job Cost` measure is JOB-type only — a new generic `Top 10 Resource Cost` measure is required for Page 3

---

## Narrative
- **Core story:** How much is Databricks costing, who is spending it, and which resources are the biggest drivers?
- **Audience promise:** In 3 pages, a platform engineer or FinOps analyst can see total spend and trend, break it down by workspace/identity/product mix, and drill into the specific resources burning the most budget.
- **Key questions answered:**
  - What is our total Databricks cost this month vs last month and vs last year?
  - Which workspaces are the biggest spenders?
  - How much of our cost is serverless vs classic compute?
  - Which jobs, clusters, and warehouses are the top cost drivers?
  - Who owns those resources?

---

## Design identity
- **Tone:** Brand-forward Ipsen
- **Signature:** Dark navy header band + Ipsen teal accent on white canvas; Ipsen logo pinned to top-left on every page; tabular numerals for all KPI cards
- **Palette:**
  - Primary (Ipsen teal): `#00A0B4`
  - Secondary (dark navy): `#003087`
  - Canvas: `#FFFFFF`
  - Header background: `#003087`
  - Accent: `#00A0B4`
  - Neutral light: `#F5F5F5`
  - Border/grid: `#E0E0E0`
  - Text primary: `#1A1A1A`
  - Text on dark: `#FFFFFF`
- **Typography:** Segoe UI Bold (KPI values, chart titles), Segoe UI Regular (labels, table content)

---

## Page plan

### 1. Cost Overview
- **Archetype:** Executive Summary
- **Layout variant:** A — header band + KPI strip + dual-chart main body + filter rail
- **Variant rationale:** Header band anchors Ipsen branding; 4-card KPI strip gives instant period snapshot; trend + workspace bar in the main body tell the cost story; filter rail isolates all interactions on the right without crowding the content area.
- **Purpose:** Landing page — total cost KPIs, daily trend, and top workspaces at a glance
- **Core visuals:**
  - 4 KPI cards: Total Cost (USD), MTD, YoY %, Avg Daily Cost
  - Line chart: Daily Cost Trend (Calendar[Date] × Total Cost)
  - Horizontal bar: Top 10 Workspaces by Cost
  - Data freshness badge
- **Slicers/interactions:** Date range (Calendar[Date]), Time Aggregation (TimeCalculation), Workspace (searchable dropdown)

### 2. Cost Attribution
- **Archetype:** Analytical Canvas
- **Layout variant:** B — 2×2 quad chart layout with consistent filter rail
- **Variant rationale:** Four equal-weight chart cells give each attribution dimension (workspace, serverless/classic, resource type, identity) equal visual real estate without a dominant hero chart.
- **Purpose:** Break down cost across all attribution dimensions with full cross-filter interactivity
- **Core visuals:**
  - Horizontal bar: Cost by Workspace
  - Stacked bar (monthly): Serverless vs Classic Cost Over Time
  - Horizontal bar: Cost by Resource Type
  - Horizontal bar: Cost by Identity Type (with active identity count)
- **Slicers/interactions:** Date range, Workspace (search), Resource Type (list), Identity Type (list); all visuals cross-filter each other

### 3. Resource Detail
- **Archetype:** Analytical Canvas
- **Layout variant:** C — ranking bar + full-width detail table
- **Variant rationale:** Top horizontal bar provides the top-N ranking summary; wide table below provides the full attributable record list with owner and status columns for action.
- **Purpose:** Operational drilldown — which specific resources are driving cost, who owns them, and are they still active?
- **Core visuals:**
  - Horizontal bar: Top 10 Resources by Cost (colour by resource type)
  - Table: Resource Name, Resource Type, Total Cost, Cost %, Owned By, Is Active
- **Slicers/interactions:** Date range, Resource Type (list), Workspace (search), Active Status (tile)

---

## Design system summary
- **Theme:** Fluent2-CY26SU05 (base) with Ipsen brand overrides — dark navy (#003087) header, teal (#00A0B4) accent on data series and card borders
- **Color semantics:** Ipsen teal (#00A0B4) = primary cost series / highlights; dark navy (#003087) = header band / branding; neutral grey = secondary dimension bars
- **Typography pairing:** Segoe UI Bold (display/KPI) + Segoe UI Regular (body/labels)
- **Layout pattern:** 1920×1080 FHD; 400px right filter rail on all pages; 80px header band on all pages; 80px footer on all pages; content width = 1520px
- **Accessibility:** WCAG AA contrast on all text/background pairs; alt text on all charts; searchable dropdown for Workspace slicer (high cardinality); list slicer for Resource Type and Identity Type (≤ 5 items)

---

## Model requirements

### Existing measures (no changes needed)
- `Total Cost (USD)`, `Total Cost (USD) (mtd)`, `Total Cost (USD) (ytd)`, `Cost (USD) YoY (%)`, `Avg Daily Cost (USD)`
- `Cost vs Prior Month (USD)`, `Cost vs Prior Month (%)`
- `# Active Workspaces`, `# Active Identities`, `# Active Resources`
- `Serverless Cost (USD)`, `Serverless Cost (%)`, `Photon Cost (USD)`, `Photon Cost (%)`
- `Top 10 Workspace Cost (USD)`, `Top 10 Job Cost (USD)`, `Job Rank (by Cost)`
- `Data Latest Date`, `Is Slicer Filtered`, `Days with Cost`, `Peak Daily Cost (USD)`

### New measures required (2)
1. **`Top 10 Resource Cost (USD)`** — generic resource ranking across all types (not JOB-only), for Page 3 ranking bar:
   ```dax
   Top 10 Resource Cost (USD) =
   IF (
       RANKX ( ALL ( Resource[Resource Name] ), [Total Cost (USD)],, DESC, DENSE ) <= 10,
       [Total Cost (USD)]
   )
   ```
   Display folder: `Cost\Rankings`; format: `$#,##0.00`

2. **`Cost % of Total (Resource)`** — cost share per resource row for the Page 3 detail table:
   ```dax
   Cost % of Total (Resource) =
   DIVIDE ( [Total Cost (USD)], CALCULATE ( [Total Cost (USD)], ALL ( Resource ) ) )
   ```
   Display folder: `Cost\Rankings`; format: `0.0%`

### Calendar table assumption
The `Calendar` table is assumed to include a `Month Year` or `Month` column for month-grain grouping on the Serverless vs Classic chart. Verify before authoring Page 2.

---

## Canonical design contract

```yaml
Design Brief:
  generated_by: powerbi-report-design
  contract_version: "1.0"

  design_identity:
    tone: Brand-forward Ipsen
    signature: "Dark navy header band, Ipsen teal accent, white canvas, logo pinned top-left every page, tabular numerals"
    palette:
      primary: "#00A0B4"
      secondary: "#003087"
      canvas: "#FFFFFF"
      header_bg: "#003087"
      accent: "#00A0B4"
      neutral_light: "#F5F5F5"
      border: "#E0E0E0"
      text_primary: "#1A1A1A"
      text_on_dark: "#FFFFFF"

  typography:
    display: "Segoe UI Bold"
    body: "Segoe UI"
    kpi_value_size: 28
    chart_title_size: 14
    label_size: 11
    caption_size: 9

  accessibility:
    contrast_minimum: WCAG AA
    alt_text_required: true
    slicer_style_rule: "dropdown+search for >20 items; list for ≤5 items; tile for boolean/flag"

  pages:

    - name: "Cost Overview"
      archetype: Executive Summary
      layout_variant: A
      variant_rationale: "Header band + KPI strip + dual-chart body + filter rail. KPI strip gives instant period snapshot; trend + workspace bar tell the cost story; filter rail isolates all interactions."

      layout_contract:
        canvas:
          width: 1920
          height: 1080

        grid:
          regions:
            - name: header
              x: 0
              y: 0
              width: 1920
              height: 80
              role: "Branding — dark navy band, page title, Ipsen logo, data freshness badge"
            - name: kpi-strip
              x: 0
              y: 80
              width: 1520
              height: 160
              role: "4 KPI cards — Total Cost, MTD, YoY %, Avg Daily Cost"
            - name: filter-rail
              x: 1520
              y: 80
              width: 400
              height: 840
              role: "Slicers stacked: date range, time aggregation, workspace"
            - name: trend-area
              x: 0
              y: 240
              width: 900
              height: 760
              role: "Primary story — daily cost trend line chart"
            - name: workspace-bar-area
              x: 900
              y: 240
              width: 620
              height: 760
              role: "Secondary — top 10 workspaces horizontal bar chart"
            - name: footer
              x: 0
              y: 1000
              width: 1920
              height: 80
              role: "Data freshness label and page description"

        placements:
          - visual_type: textbox
            name: page_title
            region: header
            position: { x: 600, y: 10, width: 720, height: 60 }
            title: "Cost Overview"
            style: { color: "#FFFFFF", fontSize: 22, fontWeight: bold, align: center }

          - visual_type: image
            name: logo
            region: header
            position: { x: 20, y: 10, width: 200, height: 60 }
            source: "Logo-Ipsen-RGB20244055944266548.png"

          - visual_type: cardVisual
            name: kpi_total_cost
            region: kpi-strip
            position: { x: 0, y: 80, width: 380, height: 160 }
            measure: "KPIs[Total Cost (USD)]"
            label: "Total Cost"
            context: "Sub-label: KPIs[Cost vs Prior Month (%)] — composite KPI treatment"

          - visual_type: cardVisual
            name: kpi_mtd
            region: kpi-strip
            position: { x: 380, y: 80, width: 380, height: 160 }
            measure: "KPIs[Total Cost (USD) (mtd)]"
            label: "Month-to-Date"

          - visual_type: cardVisual
            name: kpi_yoy
            region: kpi-strip
            position: { x: 760, y: 80, width: 380, height: 160 }
            measure: "KPIs[Cost (USD) YoY (%)]"
            label: "Year-over-Year"

          - visual_type: cardVisual
            name: kpi_avg_daily
            region: kpi-strip
            position: { x: 1140, y: 80, width: 380, height: 160 }
            measure: "KPIs[Avg Daily Cost (USD)]"
            label: "Avg Daily Cost"

          - visual_type: lineChart
            name: daily_cost_trend
            region: trend-area
            position: { x: 0, y: 240, width: 900, height: 760 }
            x_axis: "Calendar[Date]"
            values: ["KPIs[Total Cost (USD)]"]
            title: "Daily Cost Trend"
            alt_text: "Line chart showing daily Databricks cost over time"

          - visual_type: barChart
            name: top_workspaces
            region: workspace-bar-area
            position: { x: 900, y: 240, width: 620, height: 760 }
            category: "Workspace[Workspace Name]"
            values: ["KPIs[Top 10 Workspace Cost (USD)]"]
            title: "Top 10 Workspaces by Cost"
            orientation: horizontal
            alt_text: "Horizontal bar chart of top 10 workspaces ranked by total cost"

          - visual_type: slicer
            name: slicer_date_range
            region: filter-rail
            position: { x: 1520, y: 80, width: 400, height: 200 }
            field: "Calendar[Date]"
            style: "between (date range picker)"

          - visual_type: slicer
            name: slicer_time_agg
            region: filter-rail
            position: { x: 1520, y: 300, width: 400, height: 200 }
            field: "TimeCalculation[Time Aggregation]"
            style: "tile"

          - visual_type: slicer
            name: slicer_workspace
            region: filter-rail
            position: { x: 1520, y: 520, width: 400, height: 240 }
            field: "Workspace[Workspace Name]"
            style: "dropdown search"

          - visual_type: cardVisual
            name: data_freshness
            region: footer
            position: { x: 20, y: 1010, width: 600, height: 60 }
            measure: "KPIs[Data Latest Date]"
            label: "Data as of"
            is_composite_kpi: true
            context: "Data freshness indicator in footer — not the hero or dominant visual"

        space_audit:
          unplaced_regions: []
          balance_rationale: >
            Filter rail (400×840px) anchors all interactions on the right.
            KPI strip spans 1520px as 4 equal 380px cards — no card dominates.
            Trend chart (900×760) takes 59% of the main body to emphasise
            time series as the primary story; workspace bar (620×760) takes
            41% as supporting context. Footer (80px) holds data freshness only.
            No wasted space; each region has at least one visual.

    - name: "Cost Attribution"
      archetype: Analytical Canvas
      layout_variant: B
      variant_rationale: "2x2 quad chart layout gives equal weight to all four attribution dimensions. No KPI strip needed — Page 1 already establishes total cost context."

      layout_contract:
        canvas:
          width: 1920
          height: 1080

        grid:
          regions:
            - name: header
              x: 0
              y: 0
              width: 1920
              height: 80
              role: "Branding — dark navy band, page title, Ipsen logo"
            - name: filter-rail
              x: 1520
              y: 80
              width: 400
              height: 920
              role: "Slicers stacked: date range, workspace, resource type, identity type"
            - name: top-left-chart
              x: 0
              y: 80
              width: 760
              height: 460
              role: "Cost by Workspace horizontal bar"
            - name: top-right-chart
              x: 760
              y: 80
              width: 760
              height: 460
              role: "Serverless vs Classic stacked bar (monthly)"
            - name: bottom-left-chart
              x: 0
              y: 540
              width: 760
              height: 460
              role: "Cost by Resource Type horizontal bar"
            - name: bottom-right-chart
              x: 760
              y: 540
              width: 760
              height: 460
              role: "Cost by Identity Type horizontal bar"
            - name: footer
              x: 0
              y: 1000
              width: 1520
              height: 80
              role: "Page label / navigation breadcrumb"

        placements:
          - visual_type: textbox
            name: page_title
            region: header
            position: { x: 600, y: 10, width: 720, height: 60 }
            title: "Cost Attribution"
            style: { color: "#FFFFFF", fontSize: 22, fontWeight: bold, align: center }

          - visual_type: image
            name: logo
            region: header
            position: { x: 20, y: 10, width: 200, height: 60 }
            source: "Logo-Ipsen-RGB20244055944266548.png"

          - visual_type: barChart
            name: cost_by_workspace
            region: top-left-chart
            position: { x: 0, y: 80, width: 760, height: 460 }
            category: "Workspace[Workspace Name]"
            values: ["KPIs[Total Cost (USD)]"]
            title: "Cost by Workspace"
            orientation: horizontal
            alt_text: "Horizontal bar chart of total cost by workspace"

          - visual_type: stackedBarChart
            name: serverless_vs_classic
            region: top-right-chart
            position: { x: 760, y: 80, width: 760, height: 460 }
            category: "Calendar[Date] (month grain)"
            series: "Costs[Is Serverless]"
            values: ["KPIs[Total Cost (USD)]"]
            title: "Serverless vs Classic Cost Over Time"
            alt_text: "Stacked bar chart comparing serverless and classic compute cost by month"

          - visual_type: barChart
            name: cost_by_resource_type
            region: bottom-left-chart
            position: { x: 0, y: 540, width: 760, height: 460 }
            category: "Resource[Resource Type]"
            values: ["KPIs[Total Cost (USD)]"]
            title: "Cost by Resource Type"
            orientation: horizontal
            alt_text: "Horizontal bar chart of cost by resource type (JOB, CLUSTER, WAREHOUSE, PIPELINE, APP)"

          - visual_type: barChart
            name: cost_by_identity_type
            region: bottom-right-chart
            position: { x: 760, y: 540, width: 760, height: 460 }
            category: "Identity[Identity Type]"
            values: ["KPIs[Total Cost (USD)]", "KPIs[# Active Identities]"]
            title: "Cost by Identity Type"
            orientation: horizontal
            alt_text: "Bar chart of total cost and active identity count by identity type (user vs service principal)"

          - visual_type: slicer
            name: slicer_date_range
            region: filter-rail
            position: { x: 1520, y: 80, width: 400, height: 180 }
            field: "Calendar[Date]"
            style: "between (date range picker)"

          - visual_type: slicer
            name: slicer_workspace
            region: filter-rail
            position: { x: 1520, y: 280, width: 400, height: 200 }
            field: "Workspace[Workspace Name]"
            style: "dropdown search"

          - visual_type: slicer
            name: slicer_resource_type
            region: filter-rail
            position: { x: 1520, y: 500, width: 400, height: 200 }
            field: "Resource[Resource Type]"
            style: "list"

          - visual_type: slicer
            name: slicer_identity_type
            region: filter-rail
            position: { x: 1520, y: 720, width: 400, height: 160 }
            field: "Identity[Identity Type]"
            style: "list"

          - visual_type: textbox
            name: page_label
            region: footer
            position: { x: 20, y: 1010, width: 400, height: 60 }
            title: "Cost Attribution — use slicers to filter any dimension"
            style: { color: "#666666", fontSize: 9 }

        space_audit:
          unplaced_regions: []
          balance_rationale: >
            2×2 quad gives equal 760×460px to each of the four attribution
            dimensions. Filter rail (400×920px) isolates all slicers.
            Footer (80px) holds a guidance label; 80px at the bottom of the
            filter rail (y=1000 to y=1080) is intentional blank padding
            separating the last slicer from the canvas edge.

    - name: "Resource Detail"
      archetype: Analytical Canvas
      layout_variant: C
      variant_rationale: "Ranking bar + full-width detail table. Users scan from the top-N bar to confirm the ranking, then read the table for owner and active-status context to take action."

      layout_contract:
        canvas:
          width: 1920
          height: 1080

        grid:
          regions:
            - name: header
              x: 0
              y: 0
              width: 1920
              height: 80
              role: "Branding — dark navy band, page title, Ipsen logo"
            - name: filter-rail
              x: 1520
              y: 80
              width: 400
              height: 920
              role: "Slicers stacked: date range, resource type, workspace, active status"
            - name: ranking-bar
              x: 0
              y: 80
              width: 1520
              height: 380
              role: "Top 10 resources horizontal bar chart (colour by resource type)"
            - name: detail-table
              x: 0
              y: 460
              width: 1520
              height: 540
              role: "Full resource detail table: name, type, cost, cost %, owner, active"
            - name: footer
              x: 0
              y: 1000
              width: 1520
              height: 80
              role: "Page label"

        placements:
          - visual_type: textbox
            name: page_title
            region: header
            position: { x: 600, y: 10, width: 720, height: 60 }
            title: "Resource Detail"
            style: { color: "#FFFFFF", fontSize: 22, fontWeight: bold, align: center }

          - visual_type: image
            name: logo
            region: header
            position: { x: 20, y: 10, width: 200, height: 60 }
            source: "Logo-Ipsen-RGB20244055944266548.png"

          - visual_type: barChart
            name: top_resources_bar
            region: ranking-bar
            position: { x: 0, y: 80, width: 1520, height: 380 }
            category: "Resource[Resource Name]"
            values: ["KPIs[Top 10 Resource Cost (USD)]"]
            legend: "Resource[Resource Type]"
            title: "Top 10 Resources by Cost"
            orientation: horizontal
            alt_text: "Horizontal bar chart of top 10 resources ranked by cost, coloured by resource type"

          - visual_type: tableEx
            name: resource_detail_table
            region: detail-table
            position: { x: 0, y: 460, width: 1520, height: 540 }
            columns:
              - "Resource[Resource Name]"
              - "Resource[Resource Type]"
              - "KPIs[Total Cost (USD)]"
              - "KPIs[Cost % of Total (Resource)]"
              - "Resource[Owned By]"
              - "Resource[Is Active]"
            sort: "KPIs[Total Cost (USD)] DESC"
            title: "Resource Cost Detail"
            alt_text: "Table showing all resources with total cost, cost share, owner, and active status"

          - visual_type: slicer
            name: slicer_date_range
            region: filter-rail
            position: { x: 1520, y: 80, width: 400, height: 180 }
            field: "Calendar[Date]"
            style: "between (date range picker)"

          - visual_type: slicer
            name: slicer_resource_type
            region: filter-rail
            position: { x: 1520, y: 280, width: 400, height: 200 }
            field: "Resource[Resource Type]"
            style: "list"

          - visual_type: slicer
            name: slicer_workspace
            region: filter-rail
            position: { x: 1520, y: 500, width: 400, height: 200 }
            field: "Workspace[Workspace Name]"
            style: "dropdown search"

          - visual_type: slicer
            name: slicer_active_status
            region: filter-rail
            position: { x: 1520, y: 720, width: 400, height: 160 }
            field: "Resource[Is Active]"
            style: "tile"

          - visual_type: textbox
            name: page_label
            region: footer
            position: { x: 20, y: 1010, width: 600, height: 60 }
            title: "Resource Detail — filter by type or workspace to narrow the ranking"
            style: { color: "#666666", fontSize: 9 }

        space_audit:
          unplaced_regions: []
          balance_rationale: >
            Ranking bar (1520×380px) provides the executive top-N summary.
            Detail table (1520×540px) provides the full operational record
            list with owner context. Filter rail (400×920px) gives 4 slicers
            without crowding. Footer (80px) holds a guidance label.
            The 460px boundary between bar and table creates a clear visual
            break — generous but intentional to avoid a cramped layout.
```

---

## Implementation notes

### Existing pages to reuse
- Rename `92e998ea37029db31045` ("Page 1") → "Cost Overview" — retain and reposition existing Identity Type/Name + Resource Type slicers into the new filter-rail layout
- Rename `6b94a8eee1812ebc735f` ("Duplicate of Template") → "Cost Attribution" — set visibility to visible
- Rename `d8389321a59702b221be` ("Duplicate of Template") → "Resource Detail" — set visibility to visible
- Page `be0b697fc0cd344afe6c` ("Template") — keep hidden as reference template

### Model changes (before building)
1. Add `Top 10 Resource Cost (USD)` measure to KPIs table (display folder: `Cost\Rankings`)
2. Add `Cost % of Total (Resource)` measure to KPIs table (display folder: `Cost\Rankings`)
3. Verify `Calendar` table has a `Month Year` or equivalent column for monthly grain on Page 2 serverless chart

### PBIR authoring order
1. Model measures → 2. Page renames and visibility → 3. Header band + logo (all pages) → 4. Filter rail slicers (all pages) → 5. Page 1 KPI strip + charts → 6. Page 2 quad charts → 7. Page 3 bar + table → 8. Footer labels

### Validation
- JSON parse all visual.json and page.json files
- Confirm `definition.pbir` points to `Databricks Costs.SemanticModel`
- Open and reload in Power BI Desktop
- Screenshot all 3 pages

### Risks
- `Costs[Is Serverless]` is INT (0/1) — needs legend formatting for readable stacked bar labels on Page 2
- `Resource[Is Active]` is INT (0/1) — tile slicer should show "Active" / "Inactive" labels (may need a calculated column or format string override)
- `Calendar[Date]` month-grain grouping on Page 2 stacked bar depends on date hierarchy or explicit month column in Calendar table
