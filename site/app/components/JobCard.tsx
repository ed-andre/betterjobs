import type { Job } from "~/services/api";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "~/components/ui/card";
import { Badge } from "~/components/ui/badge";
import { useEffect, useState } from "react";
import DOMPurify from 'dompurify';

interface JobCardProps {
  job: Job;
  onClick: () => void;
}

export function JobCard({ job, onClick }: JobCardProps) {
  const handleCardClick = () => {
    console.log("JobCard clicked:", job.job_title);
    onClick();
  };

  // Server-side HTML tag stripping
  const stripHtmlTagsServer = (html: string) => {
    if (!html) return '';

    return html
      .replace(/<style[^>]*>.*?<\/style>/gs, '')
      .replace(/<script[^>]*>.*?<\/script>/gs, '')
      .replace(/<[^>]*>/g, '')
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&mdash;/g, "—")
      .replace(/\s+/g, ' ')
      .trim();
  };

  // Client-side HTML tag stripping with DOMPurify
  const stripHtmlTagsClient = (html: string) => {
    if (!html) return '';

    const sanitized = DOMPurify.sanitize(html, {
      ALLOWED_TAGS: [],
      ALLOWED_ATTR: [],
      KEEP_CONTENT: true
    });

    return sanitized
      .replace(/&nbsp;/g, ' ')
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&mdash;/g, "—")
      .replace(/\s+/g, ' ')
      .trim();
  };

  // Choose appropriate method based on environment
  const stripHtmlTags = (html: string) => {
    if (!html) return '';

    const isBrowser = typeof window !== 'undefined' && typeof document !== 'undefined';
    return isBrowser ? stripHtmlTagsClient(html) : stripHtmlTagsServer(html);
  };

  const getDescriptionPreview = () => {
    if (!job.job_description) return 'No description available.';

    const cleanText = stripHtmlTags(job.job_description);
    return cleanText.length > 250 ? cleanText.substring(0, 250) + '...' : cleanText;
  };

  const formatDate = (dateString: string | null) => {
    if (!dateString) return 'Posted date unknown';

    try {
      const date = new Date(dateString);
      if (isNaN(date.getTime())) {
        return 'Recent';
      }
      return `Posted: ${date.toLocaleDateString()}`;
    } catch (e) {
      return 'Recent';
    }
  };

  return (
    <Card
      className="h-full flex flex-col hover:shadow-lg transition-shadow cursor-pointer"
      onClick={handleCardClick}
      role="button"
      tabIndex={0}
    >
      <CardHeader className="pb-2">
        <div className="flex justify-between items-start">
          <CardTitle
            className="text-xl text-blue-600 mb-2 cursor-pointer hover:underline"
            onClick={(e) => {
              e.stopPropagation(); // Prevent double triggering
              handleCardClick();
            }}
          >
            {job.job_title}
          </CardTitle>
          <Badge variant="outline">{job.platform}</Badge>
        </div>
        <CardDescription className="text-base font-medium">
          {job.company_name || 'Company'}
        </CardDescription>

        <div className="text-sm text-gray-500 mt-1">
          {job.location || 'Location not specified'}
        </div>
      </CardHeader>
      <CardContent className="flex-grow py-2">
        <p className="text-gray-700 text-sm line-clamp-3">
          {getDescriptionPreview()}
        </p>
      </CardContent>
      <CardFooter className="flex justify-between items-center pt-2 text-xs text-gray-500">
        <span>
          {job.department &&
            <Badge variant="secondary" className="mr-2">{job.department}</Badge>
          }
        </span>
        <span>
          {formatDate(job.date_posted)}
        </span>
      </CardFooter>
    </Card>
  );
}