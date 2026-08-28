use std::fmt::Write as _;

use anyhow::{Result, ensure};

use crate::report::{BenchmarkReport, BenchmarkRun};

const WIDTH: f64 = 1_200.0;
const LEFT: f64 = 190.0;
const RIGHT: f64 = 330.0;
const PLOT_WIDTH: f64 = WIDTH - LEFT - RIGHT;
const ROW_HEIGHT: f64 = 42.0;
const PANEL_HEADER_HEIGHT: f64 = 58.0;
const PANEL_GAP: f64 = 28.0;
const FOOTER_HEIGHT: f64 = 118.0;
const SUBTITLE_MAX_CHARACTERS: usize = 132;
const RUN_LABEL_MAX_CHARACTERS: usize = 16;
const VALUE_LABEL_MAX_CHARACTERS: usize = 36;
const MEDIAN_LABEL_MAX_CHARACTERS: usize = 40;
const FOOTER_MAX_CHARACTERS: usize = 132;

pub fn render(report: &BenchmarkReport) -> Result<String> {
    ensure!(
        !report.runs.is_empty(),
        "cannot render an empty benchmark report"
    );

    let panel_height = PANEL_HEADER_HEIGHT + report.runs.len() as f64 * ROW_HEIGHT;
    let first_panel_y = 82.0;
    let second_panel_y = first_panel_y + panel_height + PANEL_GAP;
    let footer_y = second_panel_y + panel_height + 34.0;
    let height = footer_y + FOOTER_HEIGHT;
    let scale = maximum_finite_rate(report).max(1.0);
    let mut svg = String::with_capacity(8_192);

    writeln!(
        svg,
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {WIDTH:.0} {height:.0}\" role=\"img\" aria-labelledby=\"title description\">"
    )?;
    writeln!(
        svg,
        "  <title id=\"title\">MemKafka throughput benchmark</title>"
    )?;
    writeln!(
        svg,
        "  <desc id=\"description\">Producer and end-to-end throughput for {} benchmark runs.</desc>",
        report.runs.len()
    )?;
    svg.push_str(
        "  <style>\n\
         text{font-family:ui-monospace,SFMono-Regular,Consolas,monospace;fill:#172033}\n\
         .title{font-size:28px;font-weight:700}.subtitle{font-size:14px;fill:#536078}\n\
         .panel-title{font-size:18px;font-weight:700}.axis{stroke:#c8d1df;stroke-width:1}\n\
         .producer-bar{fill:#1473e6}.end-to-end-bar{fill:#17a673}\n\
         .run-label{font-size:14px;font-weight:600}.value-label{font-size:13px}\n\
         .median-guide{stroke:#d04444;stroke-width:2;stroke-dasharray:6 5}\n\
         .median-label{font-size:12px;fill:#a12d2d}.footer{font-size:13px;fill:#536078}\n\
         </style>\n",
    );
    svg.push_str("  <rect width=\"100%\" height=\"100%\" rx=\"16\" fill=\"#f8fafc\"/>\n");
    svg.push_str("  <text class=\"title\" x=\"48\" y=\"44\">MemKafka throughput</text>\n");
    let subtitle = truncate_label(
        &format!(
            "{} records · {}-byte values · {} partitions · batches of {}",
            format_integer(report.workload.messages),
            format_integer(report.workload.payload_bytes as u64),
            report.workload.partitions,
            format_integer(report.workload.batch_records as u64),
        ),
        SUBTITLE_MAX_CHARACTERS,
    );
    writeln!(
        svg,
        "  <text class=\"subtitle\" x=\"48\" y=\"66\">{}</text>",
        escape_xml(&subtitle),
    )?;

    render_panel(
        &mut svg,
        report,
        Panel {
            title: "Producer throughput",
            y: first_panel_y,
            bar_class: "producer-bar",
            rate: |run| run.producer_records_per_second,
            gib_rate: |run| run.producer_gib_per_second,
            median_rate: report.median.producer_records_per_second,
            median_gib_rate: report.median.producer_gib_per_second,
        },
        scale,
    )?;
    render_panel(
        &mut svg,
        report,
        Panel {
            title: "End-to-end throughput",
            y: second_panel_y,
            bar_class: "end-to-end-bar",
            rate: |run| run.end_to_end_records_per_second,
            gib_rate: |run| run.end_to_end_gib_per_second,
            median_rate: report.median.end_to_end_records_per_second,
            median_gib_rate: report.median.end_to_end_gib_per_second,
        },
        scale,
    )?;

    let peak_rss = report
        .runs
        .iter()
        .map(|run| run.peak_rss_bytes)
        .max()
        .unwrap_or(0);
    let machine = truncate_label(
        &format!(
            "{} · {} {} · {} · {} logical cores",
            report.machine.cpu,
            report.machine.operating_system,
            report.machine.operating_system_version,
            report.machine.architecture,
            report.machine.logical_cores,
        ),
        FOOTER_MAX_CHARACTERS,
    );
    let identity = truncate_label(
        &format!(
            "Peak broker RSS {} GiB · commit {} · generated {}",
            format_rate(peak_rss as f64 / 1024.0_f64.powi(3)),
            report.commit,
            report.generated_at.to_rfc3339(),
        ),
        FOOTER_MAX_CHARACTERS,
    );
    writeln!(
        svg,
        "  <line class=\"axis\" x1=\"48\" x2=\"1152\" y1=\"{:.1}\" y2=\"{:.1}\"/>",
        footer_y - 22.0,
        footer_y - 22.0,
    )?;
    writeln!(
        svg,
        "  <text class=\"footer\" x=\"48\" y=\"{footer_y:.1}\">{}</text>",
        escape_xml(&machine)
    )?;
    writeln!(
        svg,
        "  <text class=\"footer\" x=\"48\" y=\"{:.1}\">{}</text>",
        footer_y + 25.0,
        escape_xml(&identity)
    )?;
    svg.push_str("</svg>\n");
    Ok(svg)
}

struct Panel {
    title: &'static str,
    y: f64,
    bar_class: &'static str,
    rate: fn(&BenchmarkRun) -> f64,
    gib_rate: fn(&BenchmarkRun) -> f64,
    median_rate: f64,
    median_gib_rate: f64,
}

fn render_panel(
    svg: &mut String,
    report: &BenchmarkReport,
    panel: Panel,
    scale: f64,
) -> Result<()> {
    writeln!(
        svg,
        "  <text class=\"panel-title\" x=\"48\" y=\"{:.1}\">{}</text>",
        panel.y + 23.0,
        panel.title,
    )?;
    let plot_top = panel.y + PANEL_HEADER_HEIGHT;
    let plot_bottom = plot_top + report.runs.len() as f64 * ROW_HEIGHT;
    writeln!(
        svg,
        "  <line class=\"axis\" x1=\"{LEFT:.1}\" x2=\"{LEFT:.1}\" y1=\"{plot_top:.1}\" y2=\"{plot_bottom:.1}\"/>"
    )?;

    for (row, run) in report.runs.iter().enumerate() {
        let row_y = plot_top + row as f64 * ROW_HEIGHT;
        let bar_y = row_y + 6.0;
        let rate = finite_nonnegative((panel.rate)(run));
        let gib_rate = finite_nonnegative((panel.gib_rate)(run));
        let bar_width = rate / scale * PLOT_WIDTH;
        let value_label = truncate_label(
            &format!(
                "{} records/s · {} GiB/s",
                format_integer(rate.round() as u64),
                format_rate(gib_rate),
            ),
            VALUE_LABEL_MAX_CHARACTERS,
        );
        let run_label = truncate_label(&format!("Run {}", run.run), RUN_LABEL_MAX_CHARACTERS);
        writeln!(
            svg,
            "  <text class=\"run-label\" x=\"48\" y=\"{:.1}\">{}</text>",
            bar_y + 17.0,
            escape_xml(&run_label),
        )?;
        writeln!(
            svg,
            "  <rect class=\"{}\" x=\"{LEFT:.1}\" y=\"{bar_y:.1}\" width=\"{bar_width:.1}\" height=\"24\" rx=\"4\"/>",
            panel.bar_class,
        )?;
        writeln!(
            svg,
            "  <text class=\"value-label\" x=\"{:.1}\" y=\"{:.1}\">{}</text>",
            LEFT + PLOT_WIDTH + 12.0,
            bar_y + 17.0,
            escape_xml(&value_label),
        )?;
    }

    let median_rate = finite_nonnegative(panel.median_rate);
    let median_gib_rate = finite_nonnegative(panel.median_gib_rate);
    let median_x = LEFT + median_rate / scale * PLOT_WIDTH;
    let median_label = truncate_label(
        &format!(
            "Median {} records/s · {} GiB/s",
            format_integer(median_rate.round() as u64),
            format_rate(median_gib_rate),
        ),
        MEDIAN_LABEL_MAX_CHARACTERS,
    );
    writeln!(
        svg,
        "  <line class=\"median-guide\" x1=\"{median_x:.1}\" x2=\"{median_x:.1}\" y1=\"{:.1}\" y2=\"{plot_bottom:.1}\"/>",
        plot_top - 7.0,
    )?;
    writeln!(
        svg,
        "  <text class=\"median-label\" x=\"{:.1}\" y=\"{:.1}\">{}</text>",
        (median_x + 6.0).min(WIDTH - 300.0),
        plot_top - 12.0,
        escape_xml(&median_label),
    )?;
    Ok(())
}

fn maximum_finite_rate(report: &BenchmarkReport) -> f64 {
    report
        .runs
        .iter()
        .flat_map(|run| {
            [
                run.producer_records_per_second,
                run.end_to_end_records_per_second,
            ]
        })
        .chain([
            report.median.producer_records_per_second,
            report.median.end_to_end_records_per_second,
        ])
        .filter(|rate| rate.is_finite() && *rate > 0.0)
        .fold(0.0, f64::max)
}

fn finite_nonnegative(value: f64) -> f64 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        0.0
    }
}

fn format_integer(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            formatted.push(',');
        }
        formatted.push(character);
    }
    formatted
}

fn format_rate(value: f64) -> String {
    format!("{:.3}", finite_nonnegative(value))
}

fn truncate_label(value: &str, maximum_characters: usize) -> String {
    if value.chars().count() <= maximum_characters {
        return value.to_owned();
    }
    let mut truncated = value
        .chars()
        .take(maximum_characters.saturating_sub(1))
        .collect::<String>();
    truncated.push('…');
    truncated
}

fn escape_xml(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            _ if is_xml_1_0_character(character) => escaped.push(character),
            _ => escaped.push_str("&#xFFFD;"),
        }
    }
    escaped
}

fn is_xml_1_0_character(character: char) -> bool {
    matches!(
        character,
        '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}'
    )
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use crate::report::{
        BenchmarkReport, BenchmarkRun, MachineMetadata, MedianMetrics, WorkloadMetadata,
    };

    fn run(index: usize, producer_rate: f64, end_to_end_rate: f64) -> BenchmarkRun {
        BenchmarkRun {
            run: index,
            topic: format!("topic-{index}"),
            broker_pid: 10_000 + index as u32,
            messages: 1_000_000,
            value_bytes: 4_096_000_000,
            producer_seconds: 1_000_000.0 / producer_rate.max(1.0),
            producer_records_per_second: producer_rate,
            producer_gib_per_second: producer_rate * 4096.0 / 1024.0_f64.powi(3),
            end_to_end_seconds: 1_000_000.0 / end_to_end_rate.max(1.0),
            end_to_end_records_per_second: end_to_end_rate,
            end_to_end_gib_per_second: end_to_end_rate * 4096.0 / 1024.0_f64.powi(3),
            peak_rss_bytes: (4_500 + index as u64 * 100) * 1024 * 1024,
        }
    }

    fn report(rates: &[(f64, f64)]) -> BenchmarkReport {
        let runs = rates
            .iter()
            .enumerate()
            .map(|(index, &(producer, end_to_end))| run(index + 1, producer, end_to_end))
            .collect::<Vec<_>>();
        BenchmarkReport {
            schema_version: 1,
            generated_at: Utc
                .with_ymd_and_hms(2026, 8, 28, 12, 34, 56)
                .single()
                .unwrap(),
            commit: "0123456789abcdef".to_owned(),
            workload: WorkloadMetadata {
                messages: 1_000_000,
                payload_bytes: 4096,
                partitions: 8,
                batch_records: 256,
            },
            machine: MachineMetadata {
                operating_system: "TestOS".to_owned(),
                operating_system_version: "1.0".to_owned(),
                architecture: "test-arch".to_owned(),
                cpu: "Test CPU".to_owned(),
                logical_cores: 8,
                total_memory_bytes: 16 * 1024 * 1024 * 1024,
                available_memory_bytes: 8 * 1024 * 1024 * 1024,
                rustc_version: "rustc 1.98.0".to_owned(),
                client_version: "rskafka 0.6.0".to_owned(),
            },
            median: MedianMetrics {
                producer_seconds: 5.0,
                producer_records_per_second: rates[rates.len() / 2].0,
                producer_gib_per_second: rates[rates.len() / 2].0 * 4096.0 / 1024.0_f64.powi(3),
                end_to_end_seconds: 6.0,
                end_to_end_records_per_second: rates[rates.len() / 2].1,
                end_to_end_gib_per_second: rates[rates.len() / 2].1 * 4096.0 / 1024.0_f64.powi(3),
                peak_rss_bytes: 4_700 * 1024 * 1024,
            },
            runs,
        }
    }

    #[test]
    fn renders_each_run_and_both_median_guides() {
        let svg = super::render(&report(&[
            (200_000.0, 180_000.0),
            (220_000.0, 190_000.0),
            (210_000.0, 185_000.0),
        ]))
        .unwrap();

        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("viewBox=\"0 0 "));
        assert!(svg.contains("Producer throughput"));
        assert!(svg.contains("End-to-end throughput"));
        assert!(svg.contains("Median"));
        assert!(svg.contains("records/s"));
        assert!(svg.contains("GiB/s"));
        assert_eq!(svg.matches("class=\"producer-bar\"").count(), 3);
        assert_eq!(svg.matches("class=\"end-to-end-bar\"").count(), 3);
        assert_eq!(svg.matches("class=\"median-guide\"").count(), 2);
        assert!(svg.ends_with('\n'));
        assert!(!svg.ends_with("\n\n"));
    }

    #[test]
    fn escapes_all_xml_metadata_characters() {
        let mut report = report(&[(100.0, 90.0), (100.0, 90.0), (100.0, 90.0)]);
        report.machine.cpu = "CPU &<>\"'".to_owned();

        let svg = super::render(&report).unwrap();

        assert!(svg.contains("CPU &amp;&lt;&gt;&quot;&apos;"), "{svg}");
        assert!(!svg.contains("CPU &<>\"'"), "{svg}");
    }

    #[test]
    fn sanitizes_xml_1_0_invalid_characters() {
        let mut report = report(&[(100.0, 90.0), (100.0, 90.0), (100.0, 90.0)]);
        report.machine.cpu = "CPU \0\u{b}\u{1f}\u{fffe}\u{ffff} tail".to_owned();

        let svg = super::render(&report).unwrap();

        assert!(
            svg.contains("CPU &#xFFFD;&#xFFFD;&#xFFFD;&#xFFFD;&#xFFFD; tail"),
            "{svg}"
        );
        assert!(svg.chars().all(|character| matches!(
            character,
            '\u{9}' | '\u{a}' | '\u{d}' | '\u{20}'..='\u{d7ff}' | '\u{e000}'..='\u{fffd}' | '\u{10000}'..='\u{10ffff}'
        )));
    }

    #[test]
    fn truncates_long_dynamic_labels_to_their_viewbox_columns() {
        let mut report = report(&[(100.0, 90.0), (100.0, 90.0), (100.0, 90.0)]);
        report.machine.cpu = "C".repeat(500);
        report.machine.operating_system = "O".repeat(500);
        report.machine.operating_system_version = "V".repeat(500);
        report.machine.architecture = "A".repeat(500);
        report.commit = "f".repeat(500);
        report.workload.messages = u64::MAX;
        report.workload.payload_bytes = usize::MAX;
        report.workload.partitions = i32::MAX;
        report.workload.batch_records = usize::MAX;
        for run in &mut report.runs {
            run.run = usize::MAX;
            run.producer_records_per_second = f64::MAX;
            run.producer_gib_per_second = f64::MAX;
            run.end_to_end_records_per_second = f64::MAX;
            run.end_to_end_gib_per_second = f64::MAX;
        }
        report.median.producer_records_per_second = f64::MAX;
        report.median.producer_gib_per_second = f64::MAX;
        report.median.end_to_end_records_per_second = f64::MAX;
        report.median.end_to_end_gib_per_second = f64::MAX;

        let svg = super::render(&report).unwrap();
        let text_for_class = |class: &str| {
            svg.lines()
                .filter(move |line| line.contains(&format!("class=\"{class}\"")))
                .map(|line| {
                    line.split_once('>')
                        .and_then(|(_, content)| content.rsplit_once('<'))
                        .map(|(content, _)| content)
                        .unwrap()
                })
                .collect::<Vec<_>>()
        };

        assert!(
            text_for_class("subtitle")
                .iter()
                .all(|label| label.chars().count() <= 132)
        );
        assert!(
            text_for_class("run-label")
                .iter()
                .all(|label| label.chars().count() <= 16)
        );
        assert!(
            text_for_class("value-label")
                .iter()
                .all(|label| label.chars().count() <= 36)
        );
        assert!(
            text_for_class("median-label")
                .iter()
                .all(|label| label.chars().count() <= 40)
        );
        assert!(
            text_for_class("footer")
                .iter()
                .all(|label| label.chars().count() <= 132)
        );
        assert!(svg.matches('…').count() >= 4, "{svg}");
        assert!(!svg.contains(&"C".repeat(500)), "{svg}");
        assert!(!svg.contains(&"f".repeat(500)), "{svg}");
    }

    #[test]
    fn renders_equal_and_zero_rates_with_finite_geometry() {
        for rates in [
            [(0.0, 0.0), (0.0, 0.0), (0.0, 0.0)],
            [(125.0, 125.0), (125.0, 125.0), (125.0, 125.0)],
        ] {
            let svg = super::render(&report(&rates)).unwrap();
            let lowercase = svg.to_ascii_lowercase();

            assert!(!lowercase.contains("nan"), "{svg}");
            assert!(!lowercase.contains("inf"), "{svg}");
            assert_eq!(svg, super::render(&report(&rates)).unwrap());
        }
    }
}
