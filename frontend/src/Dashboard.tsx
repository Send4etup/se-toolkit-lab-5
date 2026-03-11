import { useEffect, useState } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
)

interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRate {
  task: string
  avg_score: number
  attempts: number
}

interface DashboardProps {
  token: string
}

const LABS = ['lab-01', 'lab-02', 'lab-03', 'lab-04', 'lab-05']

export default function Dashboard({ token }: DashboardProps) {
  const [lab, setLab] = useState('lab-04')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRate[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  useEffect(() => {
    if (!token) return
    setLoading(true)
    setError('')

    const headers = { Authorization: `Bearer ${token}` }

    Promise.all([
      fetch(`/analytics/scores?lab=${lab}`, { headers }).then((r) => r.json()),
      fetch(`/analytics/timeline?lab=${lab}`, { headers }).then((r) => r.json()),
      fetch(`/analytics/pass-rates?lab=${lab}`, { headers }).then((r) => r.json()),
    ])
      .then(([s, t, p]: [ScoreBucket[], TimelineEntry[], PassRate[]]) => {
        setScores(s)
        setTimeline(t)
        setPassRates(p)
      })
      .catch((err: Error) => setError(err.message))
      .finally(() => setLoading(false))
  }, [token, lab])

  const scoresData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(99, 102, 241, 0.7)',
      },
    ],
  }

  const timelineData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgba(16, 185, 129, 1)',
        backgroundColor: 'rgba(16, 185, 129, 0.2)',
        tension: 0.3,
      },
    ],
  }

  return (
    <div style={{ padding: '1rem' }}>
      <div style={{ marginBottom: '1rem' }}>
        <label htmlFor="lab-select">Lab: </label>
        <select
          id="lab-select"
          value={lab}
          onChange={(e) => setLab(e.target.value)}
        >
          {LABS.map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>
      </div>

      {loading && <p>Loading...</p>}
      {error && <p style={{ color: 'red' }}>Error: {error}</p>}

      {!loading && !error && (
        <>
          <h2>Score Distribution</h2>
          <div style={{ maxWidth: '600px' }}>
            <Bar data={scoresData} />
          </div>

          <h2>Submissions Over Time</h2>
          <div style={{ maxWidth: '600px' }}>
            <Line data={timelineData} />
          </div>

          <h2>Pass Rates by Task</h2>
          <table>
            <thead>
              <tr>
                <th>Task</th>
                <th>Avg Score</th>
                <th>Attempts</th>
              </tr>
            </thead>
            <tbody>
              {passRates.map((p) => (
                <tr key={p.task}>
                  <td>{p.task}</td>
                  <td>{p.avg_score}</td>
                  <td>{p.attempts}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  )
}