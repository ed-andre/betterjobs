import type { MetaFunction } from "react-router";
import { useLoaderData } from "react-router";
import { getJobs } from "~/services/api";
import type { Job } from "~/services/api";

export const meta: MetaFunction = () => {
  return [
    { title: "Job Listings" },
    { name: "description", content: "Browse available job opportunities." },
  ];
};

export async function loader() {
  const jobs = await getJobs();
  return new Response(JSON.stringify({ jobs }), {
    headers: { "Content-Type": "application/json" },
  });
}

export default function JobsPage() {
  const { jobs } = useLoaderData() as { jobs: Job[] };

  return (
    <div style={{ fontFamily: "system-ui, sans-serif", lineHeight: "1.8", padding: "20px" }}>
      <h1 style={{ marginBottom: "2rem", textAlign: "center", color: "#333" }}>Job Listings</h1>
      {(jobs && jobs.length === 0) ? (
        <p style={{ textAlign: "center", color: "#777" }}>No job openings at the moment. Please check back later!</p>
      ) : (
        <ul style={{ listStyleType: "none", padding: 0 }}>
          {jobs && jobs.map((job: Job) => (
            <li key={job.id} style={{
              border: "1px solid #eee",
              padding: "1.5rem",
              marginBottom: "1rem",
              borderRadius: "8px",
              boxShadow: "0 2px 4px rgba(0,0,0,0.1)",
              backgroundColor: "#fff"
            }}>
              <h2 style={{ marginTop: 0, marginBottom: "0.5rem", color: "#007bff" }}>{job.job_title}</h2>
              <p style={{ margin: "0.25rem 0", color: "#555" }}><strong>Company:</strong> {job.company_name}</p>
              <p style={{ margin: "0.25rem 0", color: "#555" }}><strong>Location:</strong> {job.location || 'Not specified'}</p>
              <p style={{ margin: "0.5rem 0", color: "#666" }}>{job.job_description ? job.job_description.substring(0,100) + "..." : "No description available."}</p>
              <p style={{ margin: "0.25rem 0", fontSize: "0.9em", color: "#888" }}>
                Posted: {job.date_posted ? new Date(job.date_posted).toLocaleDateString() : 'Unknown date'}
              </p>
              <a
                href={job.job_url}
                target="_blank"
                rel="noopener noreferrer"
                style={{
                  display: "inline-block",
                  marginTop: "1rem",
                  padding: "0.5rem 1rem",
                  backgroundColor: "#007bff",
                  color: "white",
                  textDecoration: "none",
                  borderRadius: "4px"
                }}
              >
                View Job
              </a>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}