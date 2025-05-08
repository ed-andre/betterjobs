import type { Job } from "~/services/api";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetClose } from "~/components/ui/sheet";
import { Button } from "~/components/ui/button";
import { Badge } from "~/components/ui/badge";
import { useEffect } from "react";
import parse, { Element } from 'html-react-parser';
import type { HTMLReactParserOptions } from 'html-react-parser';
import DOMPurify from 'dompurify';

// CSS for job description formatting
const jobDescriptionStyles = `
.job-description-content {
  font-family: ui-sans-serif, system-ui, sans-serif;
  line-height: 1.6;
}
.job-description-content p {
  margin-bottom: 1rem;
}
.job-description-content ul,
.job-description-content ol {
  margin-left: 1.5rem;
  margin-bottom: 1rem;
  list-style-position: outside;
}
.job-description-content ul {
  list-style-type: disc;
}
.job-description-content ol {
  list-style-type: decimal;
}
.job-description-content li {
  margin-bottom: 0.5rem;
}
.job-description-content h1,
.job-description-content h2,
.job-description-content h3,
.job-description-content h4 {
  margin-top: 1.5rem;
  margin-bottom: 1rem;
  font-weight: 600;
  line-height: 1.3;
}
.job-description-content a {
  color: #3b82f6;
  text-decoration: underline;
}
.job-description-content strong,
.job-description-content b {
  font-weight: 600;
}
.job-description-content em,
.job-description-content i {
  font-style: italic;
}
.job-description-content hr {
  margin: 1.5rem 0;
  border: 0;
  border-top: 1px solid #e5e7eb;
}
.job-description-content div {
  margin-bottom: 1rem;
}`;

interface JobDetailSheetProps {
  job: Job | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function JobDetailSheet({ job, open, onOpenChange }: JobDetailSheetProps) {
  useEffect(() => {
    if (job) {
      console.log("JobDetailSheet - Job selected:", job.job_title);
      console.log("JobDetailSheet - Open state:", open);
    }
  }, [job, open]);

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

  // HTML parsing options for styling elements
  const parseOptions: HTMLReactParserOptions = {
    replace: (domNode) => {
      if (domNode instanceof Element) {
        // Remove problematic classes with tildes
        if (domNode.attribs && domNode.attribs.class) {
          if (domNode.attribs.class.includes('~')) {
            domNode.attribs.class = domNode.attribs.class.replace(/~[^~]*~/g, '');
          }
        }

        // Style headings consistently
        if (domNode.name === 'h1' || domNode.name === 'h2' || domNode.name === 'h3' || domNode.name === 'h4') {
          const headingClass = 'font-semibold mt-4 mb-2';
          domNode.attribs.class = domNode.attribs.class
            ? `${domNode.attribs.class} ${headingClass}`
            : headingClass;
        }

        // Style lists consistently
        if (domNode.name === 'ul' || domNode.name === 'ol') {
          const listClass = 'ml-5 mb-4';
          domNode.attribs.class = domNode.attribs.class
            ? `${domNode.attribs.class} ${listClass}`
            : listClass;
        }

        // Style list items consistently
        if (domNode.name === 'li') {
          const liClass = 'mb-1';
          domNode.attribs.class = domNode.attribs.class
            ? `${domNode.attribs.class} ${liClass}`
            : liClass;
        }
      }
      return undefined;
    }
  };

  if (!job) return null;

  // Server-side cleaning without DOMPurify
  const cleanHtmlServer = (html: string) => {
    if (!html) return '';

    return html
      .replace(/class="~[^"]*~/g, 'class="')
      .replace(/(\r\n|\n|\r)/gm, ' ')
      .replace(/&nbsp;/g, ' ')
      .trim();
  };

  // Client-side cleaning with DOMPurify
  const cleanHtmlClient = (html: string) => {
    if (!html) return '';

    let cleanedHtml = html
      .replace(/class="~[^"]*~/g, 'class="')
      .replace(/(\r\n|\n|\r)/gm, ' ')
      .replace(/&nbsp;/g, ' ')
      .trim();

    return DOMPurify.sanitize(cleanedHtml, {
      USE_PROFILES: { html: true },
      ALLOWED_TAGS: [
        'p', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
        'ul', 'ol', 'li', 'a', 'strong', 'em', 'b', 'i',
        'div', 'span', 'hr', 'blockquote', 'code', 'pre'
      ],
      ALLOWED_ATTR: ['href', 'target', 'rel', 'class', 'id', 'style'],
      FORBID_TAGS: ['script', 'style', 'iframe', 'form', 'input'],
      FORBID_ATTR: ['onerror', 'onload', 'onclick', 'onmouseover'],
      KEEP_CONTENT: true,
    });
  };

  // Choose appropriate sanitization based on environment
  const sanitizeHtml = (html: string) => {
    const isBrowser = typeof window !== 'undefined' && typeof document !== 'undefined';
    return isBrowser ? cleanHtmlClient(html) : cleanHtmlServer(html);
  };

  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent className="w-full md:max-w-3xl lg:max-w-4xl overflow-y-auto p-0">
        <div className="h-full flex flex-col">
          {/* Header Section */}
          <SheetHeader className="p-6 pb-2 border-b">
            <div className="flex justify-between items-start mb-2 mt-5">
              <SheetTitle className="text-2xl font-bold text-blue-700">{job.job_title}</SheetTitle>
              <Button
                className="bg-blue-600 hover:bg-blue-700 whitespace-nowrap"
                onClick={() => window.open(job.job_url, '_blank')}
              >
                Apply Now
              </Button>
            </div>

            <div className="flex flex-col md:flex-row md:items-center justify-between gap-2 mb-2">
              <SheetDescription className="text-base font-medium m-0">
                {job.company_name || 'Company'} • {job.location || 'Location not specified'}
              </SheetDescription>

              <div className="text-sm text-gray-500">
                {formatDate(job.date_posted)}
              </div>
            </div>

            <div className="flex flex-wrap gap-2 mt-2">
              <Badge variant="outline">{job.platform}</Badge>
              {job.department &&
                <Badge variant="secondary">{job.department}</Badge>
              }
            </div>
          </SheetHeader>

          {/* Body Section */}
          <div className="flex-grow overflow-y-auto p-6">
            <h3 className="text-lg font-semibold mb-4">Job Description</h3>
            <div className="prose max-w-none text-gray-700 job-description">
              {job.job_description ? (
                <div className="job-description-content">
                  {parse(sanitizeHtml(job.job_description), parseOptions)}
                </div>
              ) : (
                <p>No description available.</p>
              )}
            </div>

            <div className="mt-4 text-sm text-gray-500">
              Job ID: {job.job_id}
            </div>
          </div>

          {/* Footer Section */}
          <div className="p-6 pt-3 border-t mt-auto">
            <SheetClose asChild>
              <Button variant="outline" className="w-full">Close</Button>
            </SheetClose>
          </div>
        </div>

        {/* Add custom styles for job description content */}
        <style dangerouslySetInnerHTML={{ __html: jobDescriptionStyles }} />
      </SheetContent>
    </Sheet>
  );
}