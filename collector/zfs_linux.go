package collector

import (
	"github.com/mistifyio/go-zfs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
)

var (
	zfsLabelNames = []string{"name", "mountpoint"}
)

type zfsCollector struct {
	usedDesc, availDesc, referDesc *prometheus.Desc
}

type zfsStats struct {
	labelValues        []string
	used, avail, refer float64
}

func init() {
	Factories["zfs"] = NewZfsCollector
}

func NewZfsCollector() (Collector, error) {
	subsystem := "zfs"

	usedDesc := prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, subsystem, "size"),
		"zfs filesystem used in bytes.",
		zfsLabelNames, nil,
	)

	availDesc := prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, subsystem, "avail"),
		"zfs filesystem available in bytes.",
		zfsLabelNames, nil,
	)

	referDesc := prometheus.NewDesc(
		prometheus.BuildFQName(Namespace, subsystem, "refer"),
		"zfs filesystem referred size in bytes.",
		zfsLabelNames, nil,
	)

	return &zfsCollector{
		usedDesc:  usedDesc,
		availDesc: availDesc,
		referDesc: referDesc,
	}, nil
}

func (c *zfsCollector) Update(ch chan<- prometheus.Metric) (err error) {
	stats, err := c.GetStats()
	if err != nil {
		return err
	}
	for _, s := range stats {
		ch <- prometheus.MustNewConstMetric(
			c.usedDesc, prometheus.GaugeValue,
			s.used, s.labelValues...,
		)
		ch <- prometheus.MustNewConstMetric(
			c.availDesc, prometheus.GaugeValue,
			s.avail, s.labelValues...,
		)
		ch <- prometheus.MustNewConstMetric(
			c.referDesc, prometheus.GaugeValue,
			s.refer, s.labelValues...,
		)
	}
	return nil
}

func (c *zfsCollector) GetStats() (stats []zfsStats, err error) {
	stats = []zfsStats{}
	f, err := zfs.Filesystems("")
	if err != nil {
		log.Errorf("error reading filesystems: %s", err)
	}

	for _, fs := range f {
		labelValues := []string{fs.Name, fs.Mountpoint}
		stats = append(stats, zfsStats{
			labelValues: labelValues,
			used:        float64(fs.Used),
			avail:       float64(fs.Avail),
			refer:       float64(fs.Logicalused),
		})
	}
	return stats, nil
}
